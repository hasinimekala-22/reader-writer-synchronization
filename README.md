SYNC LAB — Reader–Writer Synchronization Simulator
A C and GTK4 desktop application for demonstrating the classic Reader–Writer synchronization problem using POSIX threads, a mutex, and a semaphore.
Project overview
The application models multiple reader threads accessing a shared resource and writer threads updating it. Readers may read concurrently, while a writer must acquire exclusive access before changing the shared value.
- Initial shared value: 100
- Reader threads: 4
- Writer threads: 2
- Each writer adds: 10
- Expected final value after a successful run: 120
The GUI shows the current resource value, reader/writer states, completed writes, worker completion progress, and an execution log.
Features
- Clean GTK4 desktop interface
- Real POSIX reader and writer threads
- Mutex-protected reader-count bookkeeping
- Semaphore-protected shared-resource access
- Reader and writer state cards
- Timestamped execution log
- Start, Pause, Resume, Step, and Reset controls
- Step mode and adjustable simulation speed
- Final-state check for the expected value of 120
Requirements
- Windows
- MSYS2 with the UCRT64 environment
- GCC toolchain
- GTK4 development package
- pkg-config
- POSIX thread and semaphore support provided by the toolchain/runtime
Install the required packages in the MSYS2 UCRT64 terminal if needed:
pacman -S --needed mingw-w64-ucrt-x86_64-gtk4 mingw-w64-ucrt-x86_64-toolchain
Build and run
Open the MSYS2 UCRT64 terminal and navigate to the project folder. For example:
cd /c/Users/M_Hasini/Desktop/reader-writer-synchronization
Compile the GUI:
gcc reader_writer_gui.c -o reader_writer_gui.exe $(pkg-config --cflags --libs gtk4) -pthread
Run it:
./reader_writer_gui.exe
How synchronization works
Readers
A mutex protects the shared reader count. When the first reader enters, it waits for the resource semaphore. Other readers can enter while that reader group holds the resource. When the last reader exits, it releases the semaphore.
Writers
A writer waits on the resource semaphore before writing. This provides exclusive access to the shared resource. Each writer adds 10 to the current value and then releases the semaphore.
Synchronization primitives
- Mutex: protects reader-count bookkeeping from concurrent updates.
- Semaphore: ensures that the reader group and a writer do not access the shared resource at the same time, and prevents two writers from writing simultaneously.
- Condition variable: supports pause/resume and step-mode checkpoints.
Expected result
A successful full run should complete all six worker threads and show:
Initial value: 100
Writer update: 100 -> 110
Writer update: 110 -> 120
Final value: 120
Threads completed: 6 / 6
Status: PASSED
The order of reader and writer events can vary because the threads run concurrently.
Controls
- Start: starts a new simulation from the initial value.
- Pause: asks worker threads to pause at their next checkpoint.
- Resume: releases paused workers.
- Step: releases a step-mode checkpoint.
- Enable step mode: makes workers wait for step tokens at checkpoints.
- Speed: changes the delay used by the simulation.
- Reset: restores the display and counters after the workers have finished.
Project files
reader-writer-synchronization/
├── reader_writer.c       # Console implementation, if included
├── reader_writer_gui.c   # GTK4 GUI implementation
└── README.md             # Project documentation
Include reader_writer.c in the repository only if it is part of your final submission. Do not commit temporary test files, generated executables, or build artifacts.
Academic note
This project is an educational simulation of the Reader–Writer synchronization problem. It demonstrates coordination between concurrent threads and protection of shared data using synchronization primitives.