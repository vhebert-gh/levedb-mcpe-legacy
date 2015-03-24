// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <deque>

#include <windows.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <io.h>
#include "leveldb/env.h"
#include "leveldb/slice.h"

#include "util/win_logger.h"
#include "port/port.h"
#include "util/logging.h"


#include <fstream>
#include <algorithm>
#include <sstream>
#include <chrono>
#include <ctime>
#include <memory>
#include <condition_variable>
#include <thread>

//using namespace Windows::Storage;
//using namespace Windows::ApplicationModel;

#define MAX_FILENAME 512

namespace leveldb {
	namespace {

		struct IOException : public std::exception
		{
			std::string s;
			IOException(std::string ss) : s(ss) {}
			~IOException() throw () {} // Updated
			const char* what() const throw() { return s.c_str(); }
		};
// 
// 		static std::wstring s2ws(const std::string& s)
// 		{
// 			int len;
// 			int slength = (int)s.length() + 1;
// 			len = MultiByteToWideChar(CP_UTF8, 0, s.c_str(), slength, 0, 0);
// 			wchar_t* buf = new wchar_t[len];
// 			MultiByteToWideChar(CP_UTF8, 0, s.c_str(), slength, buf, len);
// 			std::wstring r(buf);
// 			delete[] buf;
// 			return r;
// 		}

		static std::string ws2s(const std::wstring& ws)
		{
			int len;
			int wslength = (int)ws.length() + 1;
			len = WideCharToMultiByte(CP_UTF8, 0, ws.c_str(), wslength, 0, 0, NULL, NULL);
			char* buf = new char[len];
			WideCharToMultiByte(CP_UTF8, 0, ws.c_str(), wslength, buf, len, NULL, NULL);
			std::string r(buf);
			delete[] buf;
			return r;
		}

		static Status GetLastWindowsError(const std::string& name) {
			WCHAR lpBuffer[256] = L"?";
			FormatMessageW(FORMAT_MESSAGE_FROM_SYSTEM,                 // It´s a system error
				NULL,                                      // No string to be formatted needed
				GetLastError(),                               // Hey Windows: Please explain this error!
				MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),  // Do it in the standard language
				lpBuffer,              // Put the message here
				sizeof(lpBuffer) - 1,                     // Number of bytes to store the message
				NULL);
			return Status::IOError(name, ws2s(lpBuffer).c_str());
		}

		static std::wstring GetFullPath(const std::string& fname)
		{
#if 0
			Platform::String^ path = Platform::String::Concat(Windows::Storage::ApplicationData::Current->LocalFolder->Path, "\\");
			return Platform::String::Concat(path, ref new Platform::String(s2ws(fname).c_str()));
#else
			return std::wstring(fname.begin(), fname.end());
#endif
		}

		static void EnsureDirectory(const std::string& fname)
		{
			std::string dir = fname;
			std::replace(dir.begin(), dir.end(), '/', '\\');
			char tmpName[MAX_FILENAME];
			strcpy_s(tmpName, dir.c_str());

			// Create parent directories
			for (char* p = strchr(tmpName, '\\'); p; p = strchr(p + 1, '\\')) {
				*p = 0;
				::CreateDirectoryW(GetFullPath(tmpName).c_str(), NULL);  // may or may not already exist
				*p = '\\';
			}
		}

		static Status OpenFile(const std::string& fname, DWORD dwDesiredAccess, DWORD dwShareMode, DWORD dwCreationDisposition, HANDLE& file)
		{
			EnsureDirectory(fname);
			std::wstring path = GetFullPath(fname);
            file = ::CreateFile2(path.c_str(),
				dwDesiredAccess,
				dwShareMode,
				dwCreationDisposition, NULL);
			return (file == INVALID_HANDLE_VALUE ? GetLastWindowsError(fname) : Status::OK());
		}

		static Status CloseFile(const std::string& fname, HANDLE& file)
		{
			if (file != INVALID_HANDLE_VALUE)
			{
				BOOL ret = ::CloseHandle(file);
				file = INVALID_HANDLE_VALUE;
				return (!ret ? GetLastWindowsError(fname) : Status::OK());
			}
			else
				return Status::OK();
		}

		// returns the ID of the current process
		static uint32_t current_process_id(void) {
			return static_cast<uint32_t>(::GetCurrentProcessId());
		}

// 		// returns the ID of the current thread
// 		static uint32_t current_thread_id(void) {
// 			return static_cast<uint32_t>(::GetCurrentThreadId());
// 		}

		static char global_read_only_buf[0x8000];

		class WinSequentialFile : public SequentialFile {
		private:
			std::string _fname;
			HANDLE _file;

		public:

			WinSequentialFile(const std::string& fname)
				: _fname(fname) 
			{
				Status s = OpenFile(fname, GENERIC_READ, FILE_SHARE_READ, OPEN_EXISTING, _file);
				if (!s.ok())
					throw IOException(s.ToString().c_str());
			}

			virtual ~WinSequentialFile() 
			{ 
				CloseFile(_fname, _file);
			}

			virtual Status Read(size_t n, Slice* result, char* scratch)
			{
				DWORD dwRead;
				BOOL ret = ::ReadFile(_file, scratch, n, &dwRead, NULL);
				if (!ret)
					return GetLastWindowsError(_fname);
				*result = Slice(scratch, dwRead);
				if (dwRead < n)
				{
					LARGE_INTEGER cur, end;
					ret = ::SetFilePointerEx(_file, LARGE_INTEGER(), &cur, FILE_CURRENT);
					if (!ret)
						return GetLastWindowsError(_fname);
					ret = ::SetFilePointerEx(_file, LARGE_INTEGER(), &end, FILE_END);
					if (!ret)
						return GetLastWindowsError(_fname);
					if (end.QuadPart > cur.QuadPart)
					{
						// couldn't read enough bytes
						::SetFilePointerEx(_file, cur, NULL, FILE_CURRENT);
						return Status::IOError(_fname, "Couldn't read all data");
					}
					else
						return Status::OK();
				}
				else
					return Status::OK();

			}

			virtual Status Skip(uint64_t n) {
				LARGE_INTEGER cur;
				cur.QuadPart = n;
				return (!::SetFilePointerEx(_file, cur, NULL, FILE_CURRENT) ? GetLastWindowsError(_fname) : Status::OK());
			}
		};

		class WinRandomAccessFile : public RandomAccessFile {
		private:
			std::string _fname;
			HANDLE _file;
			mutable std::mutex _mutex;

		public:
			WinRandomAccessFile(const std::string& fname)
				: _fname(fname)
			{
				Status s = OpenFile(fname, GENERIC_READ, FILE_SHARE_READ, OPEN_EXISTING, _file);
				if (!s.ok())
					throw IOException(s.ToString().c_str());
			}

			virtual ~WinRandomAccessFile() 
			{ 
				CloseFile(_fname, _file);
			}

			virtual Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const {
				// no pread on Windows so we emulate it with a mutex
				std::unique_lock<std::mutex> lock(_mutex);
				LARGE_INTEGER cur;
				cur.QuadPart = offset;
				if (!::SetFilePointerEx(_file, cur, NULL, FILE_BEGIN))
					return GetLastWindowsError(_fname);

				DWORD dwRead = 0;
				BOOL ret = ::ReadFile(_file, scratch, n, &dwRead, NULL);
				*result = Slice(scratch, dwRead);
				lock.unlock();
				if(!ret) {
					return GetLastWindowsError(_fname);
				}
				return Status::OK();
			}
		};

		// We preallocate up to an extra megabyte and use memcpy to append new
		// data to the file.  This is safe since we either properly close the
		// file before reading from it, or for log files, the reading code
		// knows enough to skip zero suffixes.

		class WinFile : public WritableFile {

		private:
			std::string _fname;
			HANDLE _file;

		public:
			explicit WinFile(std::string fname) : _fname(fname) {
				Status s = OpenFile(fname, GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE, CREATE_ALWAYS, _file);
				if (!s.ok())
					throw IOException(s.ToString().c_str());
			}

			virtual ~WinFile() {
				Close();
			}

		private:
		public:
			virtual Status Append(const Slice& data) {
				DWORD dwWritten;
				BOOL ret = ::WriteFile(_file, data.data(), data.size(), &dwWritten, NULL);
				return ((!ret || dwWritten < data.size()) ? GetLastWindowsError(_fname) : Status::OK());
			}

			virtual Status Close() {
				return CloseFile(_fname, _file);
			}

			virtual Status Flush() {
				BOOL ret = ::FlushFileBuffers(_file);
				return (!ret ? GetLastWindowsError(_fname) : Status::OK());
			}
	
			virtual Status Sync() {
				return Flush();
			}
		};

		class WinFileLock : public FileLock {
		private:
			std::string _fname;
			HANDLE _file;
			DWORD _fileSizeHigh;
			DWORD _fileSizeLow;
		public:
			WinFileLock(const std::string& fname) 
				: _fname(fname) 
			{
				FILE_STANDARD_INFO fi;
				Status s = OpenFile(fname, GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE, OPEN_ALWAYS, _file);
				if (!s.ok())
					throw IOException(s.ToString().c_str());
				if (_file != INVALID_HANDLE_VALUE && GetFileInformationByHandleEx(_file, FILE_INFO_BY_HANDLE_CLASS::FileStandardInfo, &fi, sizeof(fi)))
				{
					_fileSizeLow = fi.EndOfFile.LowPart;
					_fileSizeHigh = fi.EndOfFile.HighPart;
					if (_fileSizeLow > 0 || _fileSizeHigh > 0)
					{
                        OVERLAPPED overlapped = { };
						if (!::LockFileEx(_file, 0, 0, _fileSizeLow, _fileSizeHigh, &overlapped))
						{
							Status s = GetLastWindowsError(fname);
							throw IOException(s.ToString().c_str());
						}
					}
				}
				else
				{
					_fileSizeLow = _fileSizeHigh = 0;
				}
			}

			~WinFileLock() 
			{
				if (_file != INVALID_HANDLE_VALUE)
				{
					if (_fileSizeLow > 0 || _fileSizeHigh > 0)
						if (!::UnlockFileEx(_file, 0, _fileSizeLow, _fileSizeHigh, NULL))
						{
							Status s = GetLastWindowsError(_fname);
						}
					CloseFile(_fname, _file);
				}
			}

		};

		class WinRTEnv : public Env {
		public:
			WinRTEnv();
			virtual ~WinRTEnv() {
				fprintf(stderr, "Destroying Env::Default()\n");
			}

			virtual Status NewSequentialFile(const std::string& fname, SequentialFile** result)
			{
				Status s;
				try {
					*result = new WinSequentialFile(fname);
				}
				catch (const IOException & e) {
					s = Status::IOError(fname, e.what());
				}
				return s;
			}

			virtual Status NewRandomAccessFile(const std::string& fname, RandomAccessFile** result)
			{
				Status s;
				try {
					*result = new WinRandomAccessFile(fname);
				}
				catch (const IOException & e) {
					s = Status::IOError(fname, e.what());
				}
				return s;
			}

			virtual Status NewWritableFile(const std::string& fname, WritableFile** result) {
				Status s;
				try {
					// will create a new empty file to write to
					*result = new WinFile(fname);
				} catch(const IOException & e) {
					s = Status::IOError(fname, e.what());
				}
				return s;
			}

			virtual bool FileExists(const std::string& fname) {
				WIN32_FILE_ATTRIBUTE_DATA fi;
				return (GetFileAttributesExW(GetFullPath(fname).c_str(), GET_FILEEX_INFO_LEVELS::GetFileExInfoStandard, &fi) ? true : false);
			}

			virtual Status GetChildren(const std::string& dir, std::vector<std::string>* result) {
				std::string path = dir;
				result->clear();

				WIN32_FIND_DATAW ffd;
				HANDLE hFind;
				path = dir + "/*";
				hFind = FindFirstFileExW(GetFullPath(path).c_str(), FINDEX_INFO_LEVELS::FindExInfoStandard, &ffd, FINDEX_SEARCH_OPS::FindExSearchNameMatch, NULL, 0);

				if(INVALID_HANDLE_VALUE == hFind) {
					return GetLastWindowsError(path);
				}

				do {
					result->push_back(ws2s(ffd.cFileName));
				} while(FindNextFileW(hFind, &ffd) != 0);

				FindClose(hFind);

				return Status::OK();
			}

			virtual Status DeleteFile(const std::string& fname) {
				if (::DeleteFileW(GetFullPath(fname).c_str()) != 0) {
					return Status::OK();
				} else {
					return GetLastWindowsError(fname);
				}
			}

#define MAX_FILENAME 512
			virtual Status CreateDir(const std::string& name) {
				EnsureDirectory(name);
				::CreateDirectoryW(GetFullPath(name).c_str(), NULL);
				return Status::OK();
			};

			virtual Status DeleteDir(const std::string& name) {
				BOOL ret = ::RemoveDirectoryW(GetFullPath(name).c_str());
				if (!ret)
					Status s = GetLastWindowsError(name);
				return Status::OK();
			};

			virtual Status GetFileSize(const std::string& fname, uint64_t* size) {
				WIN32_FILE_ATTRIBUTE_DATA fi;
				BOOL ret = GetFileAttributesExW(GetFullPath(fname).c_str(), GET_FILEEX_INFO_LEVELS::GetFileExInfoStandard, &fi);
				if (!ret)
					return GetLastWindowsError(fname);
				*size = ((uint64_t)fi.nFileSizeLow + ((uint64_t)fi.nFileSizeHigh << 32));
				return Status::OK();
			}

			virtual Status RenameFile(const std::string& src, const std::string& target) {
				std::wstring fullsrc = GetFullPath(src);
				std::wstring fulltarget = GetFullPath(target);
				::DeleteFileW(fulltarget.c_str());
				if (::MoveFileExW(fullsrc.c_str(), fulltarget.c_str(), 0) != TRUE) {
					return GetLastWindowsError(src);
				} else {
					return Status::OK();
				}
			}

			virtual Status LockFile(const std::string& fname, FileLock** lock) {
				*lock = NULL;
				if (!FileExists(fname)) {
					HANDLE file;
					Status s = OpenFile(fname, GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE, CREATE_ALWAYS, file);
					if (s.ok())
						CloseFile(fname, file);
				}
				try
				{
					*lock = new WinFileLock(fname);
				}
				catch (const IOException & e) {
					return Status::IOError(fname, e.what());
				}

				return Status::OK();
			}

			virtual Status UnlockFile(FileLock* lock) {
				delete lock;
				return Status::OK();
			}

			virtual void Schedule(void(*function)(void*), void* arg);

			virtual void StartThread(void(*function)(void* arg), void* arg);

			virtual Status GetTestDirectory(std::string* result) {
				std::stringstream ss;
				ss << "tmp/leveldb_tests/" << current_process_id();

				// Directory may already exist
				CreateDir(ss.str());

				*result = ss.str();

				return Status::OK();
			}

#ifndef WIN32
			static uint64_t gettid() {
				pthread_t tid = pthread_self();
				uint64_t thread_id = 0;
				memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
				return thread_id;
			}
#endif

			virtual Status NewLogger(const std::string& fname, Logger** result) {
				std::string path = fname;
				std::replace(path.begin(), path.end(), '/', '\\');
				FILE* f = fopen(path.c_str(), "wt");
				if(f == NULL) {
					*result = NULL;
					return Status::IOError(fname, strerror(errno));
				} else {
#ifdef WIN32
					*result = new WinLogger(f);
#else
					*result = new PosixLogger(f, &gettid);
#endif
					return Status::OK();
				}
			}

			struct timezone {
				int  tz_minuteswest; /* minutes W of Greenwich */
				int  tz_dsttime;     /* type of dst correction */
			};
#if defined(_MSC_VER) || defined(_MSC_EXTENSIONS)
#define DELTA_EPOCH_IN_MICROSECS  116444736000000000Ui64 // CORRECT
#else
#define DELTA_EPOCH_IN_MICROSECS  116444736000000000ULL // CORRECT
#endif

#if (WINDOWS_FAMILY == WINDOWS_FAMILY_PHONE_APP)
			struct timeval {
				long    tv_sec;         /* seconds */
				long    tv_usec;        /* and microseconds */
			};
#endif
			int gettimeofday(struct timeval *tv, struct timezone *tz) {
				FILETIME ft;
				uint64_t tmpres = 0;
				static int tzflag = 0;

				if(tv) {
					GetSystemTimeAsFileTime(&ft);
					tmpres |= ft.dwHighDateTime;
					tmpres <<= 32;
					tmpres |= ft.dwLowDateTime;

					/*converting file time to unix epoch*/
					tmpres /= 10;  /*convert into microseconds*/
					tmpres -= DELTA_EPOCH_IN_MICROSECS;
					tv->tv_sec = (long)(tmpres / 1000000UL);
					tv->tv_usec = (long)(tmpres % 1000000UL);
				}

				if(tz) {
					if(!tzflag) {
#if (WINDOWS_FAMILY != WINDOWS_FAMILY_PHONE_APP)
						_tzset();
#endif
						tzflag++;
					}
					tz->tz_minuteswest = _timezone / 60;
					tz->tz_dsttime = _daylight;
				}

				return 0;
			}

			virtual uint64_t NowMicros() {
				struct timeval tv;
				gettimeofday(&tv, 0);
				return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
			}

			virtual void SleepForMicroseconds(int micros) {
				std::this_thread::sleep_for(std::chrono::microseconds(micros));
			}


		private:
			void PthreadCall(const char* label, int result) {
				if(result != 0) {
					fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
					exit(1);
				}
			}

			// BGThread() is the body of the background thread
			void BGThread();

			static void BGThreadWrapper(void* arg) {
				reinterpret_cast<WinRTEnv*>(arg)->BGThread();
			}

			std::mutex mu_;
			std::condition_variable bgsignal_;
			std::unique_ptr<std::thread> bgthread_;

			// Entry per Schedule() call
			struct BGItem { void* arg; void(*function)(void*); };
			typedef std::deque<BGItem> BGQueue;
			BGQueue queue_;
		};

		WinRTEnv::WinRTEnv() {}

		void WinRTEnv::Schedule(void(*function)(void*), void* arg) {
			std::unique_lock<std::mutex> lock(mu_);

			// Start background thread if necessary
			if(!bgthread_) {
				bgthread_.reset(
					new std::thread(&BGThreadWrapper, this));
			}

			// Add to priority queue
			queue_.push_back(BGItem());
			queue_.back().function = function;
			queue_.back().arg = arg;

			lock.unlock();

			bgsignal_.notify_one();

		}

		void WinRTEnv::BGThread() {
			while(true) {
				// Wait until there is an item that is ready to run
				std::unique_lock<std::mutex> lock(mu_);

				while(queue_.empty()) {
					bgsignal_.wait(lock);
				}

				void(*function)(void*) = queue_.front().function;
				void* arg = queue_.front().arg;
				queue_.pop_front();

				lock.unlock();
				(*function)(arg);
			}
		}

		namespace {
			struct StartThreadState {
				void(*user_function)(void*);
				void* arg;
			};
		}

		int StartThreadWrapper(LPVOID lpParam) {
 			StartThreadState* state = reinterpret_cast<StartThreadState*>(lpParam);
 			state->user_function(state->arg);
 			delete state;
 			return 0;
 		}
 
		void WinRTEnv::StartThread(void(*function)(void* arg), void* arg) {
 			StartThreadState* state = new StartThreadState;
 			state->user_function = function;
 			state->arg = arg;
			_Thrd_t thrd;
			_Thrd_create(&thrd, StartThreadWrapper, state);
 		}
	}

	static INIT_ONCE g_InitOnce = INIT_ONCE_STATIC_INIT;
	static Env* default_env;
	static BOOL CALLBACK InitDefaultEnv(PINIT_ONCE InitOnce,
		PVOID Parameter,
		PVOID *lpContext) {
		::memset(global_read_only_buf, 0, sizeof(global_read_only_buf));
		default_env = new WinRTEnv;
		return TRUE;
	}

	Env* Env::Default() {
#if 0
		PVOID lpContext;
		InitOnceExecuteOnce(&g_InitOnce,          // One-time initialization structure
			InitDefaultEnv,   // Pointer to initialization callback function
			"",                 // Optional parameter to callback function (not used)
			&lpContext);          // Receives pointer to event object stored in g_InitOnce
#else
		if (default_env == NULL)
			InitDefaultEnv(NULL, NULL, NULL);
#endif

		return default_env;
	}

}
