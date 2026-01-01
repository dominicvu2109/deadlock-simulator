Features of the Demo Application
1. Multiple Prevention Strategies

No Prevention (shows deadlocks)
Resource Ordering
Timeout-Based
Try-Lock
Eliminate Hold & Wait

2. Real-Time Visualization

Wait-For Graph: Shows which processes are waiting for which resources
Resource Allocation Matrix: Shows H (Holds), W (Waits), - (Not needed)
Deadlock Detection: Automatically detects and highlights deadlocks in red

3. Activity Log

Color-coded messages (INFO, SUCCESS, WARNING, ERROR)
Timestamps for all events
Shows resource requests, acquisitions, releases, timeouts, retries

4. Statistics Dashboard

Number of processes
Completed processes
Deadlocks detected
Total retries
Average wait time
Throughput (processes/second)

5. Interactive Controls

Select prevention strategy
Choose number of processes (2-10)
Start/Stop/Reset simulation
Real-time status updates

**Usage Guide**
Basic Usage:

Select a strategy from the dropdown (start with "No Prevention" to see deadlocks)
Choose number of processes (5 is a good default)
Click "Start Simulation"
Watch the visualization update in real-time
Monitor the activity log for detailed events
Check statistics for performance metrics

Comparing Strategies:

Run with "No Prevention" - you'll likely see deadlocks
Click "Reset"
Select "Resource Ordering" - no deadlocks!
Repeat for other strategies
Compare statistics between runs

Understanding the Visualization:
Wait-For Graph:

Blue circles = Processes
Green squares = Resources
Black solid arrows = Process holds resource
Red dashed arrows = Process waits for resource
If you see a cycle of red arrows = DEADLOCK!

Allocation Matrix:

Green cells (H) = Process holds resource
Yellow cells (W) = Process waits for resource
White cells (-) = Process doesn't need resource

This demo application provides a comprehensive, visual, and interactive way to understand deadlock prevention strategies.
