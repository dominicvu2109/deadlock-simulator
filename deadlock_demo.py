"""
Deadlock Prevention Demonstration Application - FIXED VERSION
Comprehensive simulation with GUI visualization
"""

import threading
import time
import random
import queue
from enum import Enum
from typing import List, Set, Optional, Dict
from dataclasses import dataclass, field
from datetime import datetime
import tkinter as tk
from tkinter import ttk, scrolledtext
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
import networkx as nx
from collections import defaultdict

# ============================================================================
# CORE DATA STRUCTURES
# ============================================================================

class ResourceType(Enum):
    """Available resource types with ordering"""
    R1 = 1
    R2 = 2
    R3 = 3
    R4 = 4
    R5 = 5

class ProcessState(Enum):
    """Process execution states"""
    READY = "Ready"
    RUNNING = "Running"
    WAITING = "Waiting"
    COMPLETED = "Completed"
    TIMEOUT = "Timeout"
    DEADLOCKED = "Deadlocked"

class PreventionStrategy(Enum):
    """Available deadlock prevention strategies"""
    NONE = "No Prevention"
    RESOURCE_ORDERING = "Resource Ordering"
    TIMEOUT = "Timeout-Based"
    TRYLOCK = "Try-Lock"
    HOLD_AND_WAIT = "Eliminate Hold & Wait"

@dataclass
class ProcessStats:
    """Statistics for a process"""
    process_id: int
    start_time: float = 0.0
    end_time: float = 0.0
    wait_time: float = 0.0
    retry_count: int = 0
    resources_acquired: int = 0
    state: ProcessState = ProcessState.READY
    
    @property
    def total_time(self) -> float:
        if self.end_time > 0:
            return self.end_time - self.start_time
        return time.time() - self.start_time
    
    @property
    def success(self) -> bool:
        return self.state == ProcessState.COMPLETED

@dataclass
class SimulationStats:
    """Overall simulation statistics"""
    start_time: float = 0.0
    end_time: float = 0.0
    total_processes: int = 0
    completed_processes: int = 0
    deadlocks_detected: int = 0
    total_retries: int = 0
    average_wait_time: float = 0.0
    throughput: float = 0.0

# ============================================================================
# RESOURCE MANAGER - FIXED VERSION
# ============================================================================

class ResourceManager:
    """Manages resource allocation and tracking"""
    
    def __init__(self):
        self.resources: Dict[ResourceType, Optional[int]] = {
            r: None for r in ResourceType
        }
        self.lock = threading.Lock()
        self.allocation_log: List[Dict] = []
        self.waiting_processes: Dict[int, Set[ResourceType]] = defaultdict(set)
        
    def is_available(self, resource: ResourceType) -> bool:
        """Check if resource is available"""
        with self.lock:
            return self.resources[resource] is None
    
    def allocate(self, process_id: int, resource: ResourceType) -> bool:
        """Attempt to allocate resource to process"""
        with self.lock:
            if self.resources[resource] is None:
                self.resources[resource] = process_id
                self._log_event(process_id, resource, 'ALLOCATED')
                # Remove from waiting list
                if process_id in self.waiting_processes:
                    self.waiting_processes[process_id].discard(resource)
                return True
            else:
                # Add to waiting list
                self.waiting_processes[process_id].add(resource)
                return False
    
    def release(self, process_id: int, resource: ResourceType) -> bool:
        """Release resource held by process"""
        with self.lock:
            if self.resources[resource] == process_id:
                self.resources[resource] = None
                self._log_event(process_id, resource, 'RELEASED')
                return True
            return False
    
    def get_held_resources(self, process_id: int) -> List[ResourceType]:
        """Get all resources held by process"""
        with self.lock:
            return [r for r, holder in self.resources.items() 
                   if holder == process_id]
    
    def release_all(self, process_id: int) -> int:
        """Release all resources held by process"""
        held = self.get_held_resources(process_id)
        for resource in held:
            self.release(process_id, resource)
        return len(held)
    
    def get_waiting_for(self, process_id: int) -> Set[ResourceType]:
        """Get resources process is waiting for"""
        with self.lock:
            return self.waiting_processes.get(process_id, set()).copy()
    
    def _log_event(self, process_id: int, resource: ResourceType, action: str):
        """Log allocation event"""
        self.allocation_log.append({
            'timestamp': time.time(),
            'process_id': process_id,
            'resource': resource,
            'action': action
        })
    
    def get_allocation_state(self) -> Dict:
        """Get current allocation state - THREAD SAFE"""
        with self.lock:
            return {
                'allocated': {r: h for r, h in self.resources.items() if h is not None},
                'available': [r for r, h in self.resources.items() if h is None],
                'waiting': {k: v.copy() for k, v in self.waiting_processes.items()}
            }
    
    def detect_deadlock(self) -> Optional[List[int]]:
        """Detect if there's a deadlock - returns deadlocked process IDs - THREAD SAFE"""
        with self.lock:
            # Create snapshots to avoid dictionary changed during iteration
            waiting_snapshot = {k: v.copy() for k, v in self.waiting_processes.items()}
            resources_snapshot = self.resources.copy()
            
            # Build wait-for graph
            graph = defaultdict(set)
            
            # For each waiting process
            for process_id, waiting_resources in waiting_snapshot.items():
                for resource in waiting_resources:
                    holder = resources_snapshot.get(resource)
                    if holder is not None and holder != process_id:
                        # process_id waits for holder
                        graph[process_id].add(holder)
            
            if not graph:
                return None
            
            # Detect cycle using DFS
            def has_cycle_from(node, visited, rec_stack, path):
                visited.add(node)
                rec_stack.add(node)
                path.append(node)
                
                for neighbor in graph.get(node, set()):
                    if neighbor not in visited:
                        if has_cycle_from(neighbor, visited, rec_stack, path):
                            return True
                    elif neighbor in rec_stack:
                        # Found cycle
                        return True
                
                path.pop()
                rec_stack.remove(node)
                return False
            
            visited = set()
            
            # Create a copy of keys to iterate over
            nodes = list(graph.keys())
            for node in nodes:
                if node not in visited:
                    rec_stack = set()
                    path = []
                    if has_cycle_from(node, visited, rec_stack, path):
                        # Return all processes involved in waiting
                        return list(waiting_snapshot.keys())
            
            return None

# ============================================================================
# PROCESS IMPLEMENTATIONS
# ============================================================================

class BaseProcess(threading.Thread):
    """Base class for all process types"""
    
    def __init__(self, process_id: int, resource_manager: ResourceManager,
                 needed_resources: List[ResourceType], stats_queue: queue.Queue):
        super().__init__(name=f"P{process_id}", daemon=True)
        self.process_id = process_id
        self.rm = resource_manager
        self.needed_resources = needed_resources
        self.stats_queue = stats_queue
        self.stats = ProcessStats(process_id=process_id)
        self.stop_flag = threading.Event()
        
    def log(self, message: str, level: str = "INFO"):
        """Send log message to stats queue"""
        self.stats_queue.put({
            'type': 'log',
            'process_id': self.process_id,
            'message': message,
            'level': level,
            'timestamp': time.time()
        })
    
    def update_state(self, state: ProcessState):
        """Update process state"""
        self.stats.state = state
        self.stats_queue.put({
            'type': 'state_update',
            'process_id': self.process_id,
            'state': state
        })
    
    def do_work(self, duration: float = 0.2):
        """Simulate doing work with resources"""
        self.log(f"Working with resources: {[r.name for r in self.rm.get_held_resources(self.process_id)]}")
        time.sleep(duration)

class NoPreventionProcess(BaseProcess):
    """Process with no deadlock prevention - can deadlock"""
    
    def run(self):
        self.stats.start_time = time.time()
        self.update_state(ProcessState.RUNNING)
        self.log(f"Started - needs {[r.name for r in self.needed_resources]}")
        
        acquired = []
        
        try:
            # Try to acquire resources one by one - DEADLOCK PRONE
            for resource in self.needed_resources:
                if self.stop_flag.is_set():
                    break
                    
                self.log(f"Requesting {resource.name}")
                self.update_state(ProcessState.WAITING)
                
                wait_start = time.time()
                # Busy wait - can deadlock here
                while not self.rm.allocate(self.process_id, resource):
                    if self.stop_flag.is_set():
                        break
                    time.sleep(0.01)
                
                self.stats.wait_time += time.time() - wait_start
                acquired.append(resource)
                self.update_state(ProcessState.RUNNING)
                self.log(f"Acquired {resource.name}")
                time.sleep(0.05)  # Simulate some work
            
            if not self.stop_flag.is_set():
                # Do final work
                self.do_work()
                self.update_state(ProcessState.COMPLETED)
                self.log("Completed successfully", "SUCCESS")
            
        finally:
            # Release all resources
            for resource in acquired:
                self.rm.release(self.process_id, resource)
            
            self.stats.end_time = time.time()
            self.stats_queue.put({
                'type': 'process_complete',
                'stats': self.stats
            })

class ResourceOrderingProcess(BaseProcess):
    """Process using resource ordering prevention"""
    
    def run(self):
        self.stats.start_time = time.time()
        self.update_state(ProcessState.RUNNING)
        
        # CRITICAL: Sort resources by their value (rank)
        ordered_resources = sorted(self.needed_resources, key=lambda r: r.value)
        self.log(f"Started - requesting in order: {[r.name for r in ordered_resources]}")
        
        acquired = []
        
        try:
            for resource in ordered_resources:
                if self.stop_flag.is_set():
                    break
                
                self.log(f"Requesting {resource.name} (rank {resource.value})")
                self.update_state(ProcessState.WAITING)
                
                wait_start = time.time()
                while not self.rm.allocate(self.process_id, resource):
                    if self.stop_flag.is_set():
                        break
                    time.sleep(0.01)
                
                self.stats.wait_time += time.time() - wait_start
                acquired.append(resource)
                self.stats.resources_acquired += 1
                self.update_state(ProcessState.RUNNING)
                self.log(f"Acquired {resource.name}")
                time.sleep(0.03)
            
            if not self.stop_flag.is_set():
                self.do_work()
                self.update_state(ProcessState.COMPLETED)
                self.log("Completed successfully", "SUCCESS")
        
        finally:
            # Release in reverse order (good practice)
            for resource in reversed(acquired):
                self.rm.release(self.process_id, resource)
            
            self.stats.end_time = time.time()
            self.stats_queue.put({
                'type': 'process_complete',
                'stats': self.stats
            })

class TimeoutProcess(BaseProcess):
    """Process using timeout-based prevention"""
    
    def __init__(self, *args, timeout: float = 0.5, max_retries: int = 10, **kwargs):
        super().__init__(*args, **kwargs)
        self.timeout = timeout
        self.max_retries = max_retries
    
    def try_acquire_all(self) -> bool:
        """Try to acquire all resources with timeout"""
        acquired = []
        
        for resource in self.needed_resources:
            if self.stop_flag.is_set():
                break
            
            self.log(f"Requesting {resource.name}")
            resource_start = time.time()
            
            while not self.rm.allocate(self.process_id, resource):
                if self.stop_flag.is_set():
                    break
                
                # Check timeout
                if time.time() - resource_start > self.timeout:
                    self.log(f"TIMEOUT waiting for {resource.name}", "WARNING")
                    # Release everything acquired
                    for r in acquired:
                        self.rm.release(self.process_id, r)
                    return False
                
                time.sleep(0.01)
            
            acquired.append(resource)
            self.log(f"Acquired {resource.name}")
        
        return True
    
    def run(self):
        self.stats.start_time = time.time()
        self.update_state(ProcessState.RUNNING)
        self.log(f"Started - needs {[r.name for r in self.needed_resources]}, timeout={self.timeout}s")
        
        retry_count = 0
        backoff = 0.1
        
        while retry_count < self.max_retries and not self.stop_flag.is_set():
            self.update_state(ProcessState.RUNNING)
            
            if self.try_acquire_all():
                # Success!
                self.log(f"Acquired all resources on attempt {retry_count + 1}", "SUCCESS")
                self.do_work()
                
                # Release all
                for resource in self.needed_resources:
                    self.rm.release(self.process_id, resource)
                
                self.update_state(ProcessState.COMPLETED)
                self.stats.end_time = time.time()
                self.stats_queue.put({
                    'type': 'process_complete',
                    'stats': self.stats
                })
                return
            
            # Failed - retry with backoff
            retry_count += 1
            self.stats.retry_count = retry_count
            wait_time = backoff * (2 ** retry_count) * (0.5 + 0.5 * random.random())
            self.log(f"Retry {retry_count}/{self.max_retries} after {wait_time:.2f}s", "WARNING")
            self.update_state(ProcessState.TIMEOUT)
            time.sleep(wait_time)
        
        # Failed after all retries
        self.log(f"FAILED after {self.max_retries} retries", "ERROR")
        self.update_state(ProcessState.TIMEOUT)
        self.stats.end_time = time.time()
        self.stats_queue.put({
            'type': 'process_complete',
            'stats': self.stats
        })

class TryLockProcess(BaseProcess):
    """Process using try-lock (non-blocking) approach"""
    
    def __init__(self, *args, max_attempts: int = 50, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_attempts = max_attempts
    
    def try_lock_all(self) -> bool:
        """Try to lock all resources non-blockingly"""
        acquired = []
        
        for resource in self.needed_resources:
            if self.stop_flag.is_set():
                break
            
            # Non-blocking attempt
            if self.rm.allocate(self.process_id, resource):
                acquired.append(resource)
                self.log(f"Locked {resource.name}")
            else:
                # Failed - release everything immediately
                self.log(f"Failed to lock {resource.name}, releasing all", "WARNING")
                for r in acquired:
                    self.rm.release(self.process_id, r)
                return False
        
        return True
    
    def run(self):
        self.stats.start_time = time.time()
        self.update_state(ProcessState.RUNNING)
        self.log(f"Started - needs {[r.name for r in self.needed_resources]}")
        
        attempts = 0
        while attempts < self.max_attempts and not self.stop_flag.is_set():
            if self.try_lock_all():
                # Success!
                self.log(f"Locked all resources on attempt {attempts + 1}", "SUCCESS")
                self.do_work()
                
                # Release all
                for resource in self.needed_resources:
                    self.rm.release(self.process_id, resource)
                
                self.update_state(ProcessState.COMPLETED)
                self.stats.end_time = time.time()
                self.stats_queue.put({
                    'type': 'process_complete',
                    'stats': self.stats
                })
                return
            
            # Failed - backoff and retry
            attempts += 1
            self.stats.retry_count = attempts
            backoff = 0.01 * (1 + attempts * random.random())
            time.sleep(backoff)
        
        # Failed
        self.log(f"FAILED after {self.max_attempts} attempts", "ERROR")
        self.stats.end_time = time.time()
        self.stats_queue.put({
            'type': 'process_complete',
            'stats': self.stats
        })

class EliminateHoldWaitProcess(BaseProcess):
    """Process that requests all resources at once"""
    
    def run(self):
        self.stats.start_time = time.time()
        self.update_state(ProcessState.WAITING)
        self.log(f"Started - requesting ALL resources: {[r.name for r in self.needed_resources]}")
        
        # Wait until ALL resources are available
        while not self.stop_flag.is_set():
            all_available = True
            
            # Check if all are available
            for resource in self.needed_resources:
                if not self.rm.is_available(resource):
                    all_available = False
                    break
            
            if all_available:
                # Acquire all at once
                acquired = []
                for resource in self.needed_resources:
                    if self.rm.allocate(self.process_id, resource):
                        acquired.append(resource)
                    else:
                        # Something went wrong, release all
                        for r in acquired:
                            self.rm.release(self.process_id, r)
                        break
                
                if len(acquired) == len(self.needed_resources):
                    # Success!
                    self.update_state(ProcessState.RUNNING)
                    self.log(f"Acquired ALL resources at once", "SUCCESS")
                    self.do_work()
                    
                    # Release all
                    for resource in acquired:
                        self.rm.release(self.process_id, resource)
                    
                    self.update_state(ProcessState.COMPLETED)
                    break
            
            time.sleep(0.05)
        
        self.stats.end_time = time.time()
        self.stats_queue.put({
            'type': 'process_complete',
            'stats': self.stats
        })

# ============================================================================
# SIMULATION ENGINE
# ============================================================================

class SimulationEngine:
    """Manages the simulation execution"""
    
    def __init__(self, strategy: PreventionStrategy, num_processes: int = 5):
        self.strategy = strategy
        self.num_processes = num_processes
        self.rm = ResourceManager()
        self.stats_queue = queue.Queue()
        self.processes: List[BaseProcess] = []
        self.sim_stats = SimulationStats()
        self.running = False
        self.deadlock_detector_thread = None
        
    def generate_resource_needs(self) -> List[ResourceType]:
        """Generate random resource needs for a process"""
        num_resources = random.randint(2, 4)
        return random.sample(list(ResourceType), num_resources)
    
    def create_process(self, process_id: int) -> BaseProcess:
        """Create a process based on strategy"""
        resources = self.generate_resource_needs()
        
        if self.strategy == PreventionStrategy.NONE:
            return NoPreventionProcess(process_id, self.rm, resources, self.stats_queue)
        elif self.strategy == PreventionStrategy.RESOURCE_ORDERING:
            return ResourceOrderingProcess(process_id, self.rm, resources, self.stats_queue)
        elif self.strategy == PreventionStrategy.TIMEOUT:
            return TimeoutProcess(process_id, self.rm, resources, self.stats_queue,
                                timeout=0.5, max_retries=10)
        elif self.strategy == PreventionStrategy.TRYLOCK:
            return TryLockProcess(process_id, self.rm, resources, self.stats_queue,
                                max_attempts=50)
        elif self.strategy == PreventionStrategy.HOLD_AND_WAIT:
            return EliminateHoldWaitProcess(process_id, self.rm, resources, self.stats_queue)
        else:
            return NoPreventionProcess(process_id, self.rm, resources, self.stats_queue)
    
    def start_deadlock_detector(self):
        """Start background deadlock detector"""
        def detect():
            while self.running:
                try:
                    deadlocked = self.rm.detect_deadlock()
                    if deadlocked:
                        self.stats_queue.put({
                            'type': 'deadlock_detected',
                            'processes': deadlocked
                        })
                        self.sim_stats.deadlocks_detected += 1
                except Exception as e:
                    pass  # Silently handle any errors
                time.sleep(0.5)
        
        self.deadlock_detector_thread = threading.Thread(target=detect, daemon=True)
        self.deadlock_detector_thread.start()
    
    def start(self):
        """Start the simulation"""
        self.running = True
        self.sim_stats = SimulationStats()
        self.sim_stats.start_time = time.time()
        self.sim_stats.total_processes = self.num_processes
        
        # Start deadlock detector
        self.start_deadlock_detector()
        
        # Create and start processes with slight delays
        for i in range(self.num_processes):
            process = self.create_process(i + 1)
            self.processes.append(process)
            process.start()
            time.sleep(0.05)  # Stagger starts
        
        self.stats_queue.put({
            'type': 'simulation_started',
            'strategy': self.strategy.value,
            'num_processes': self.num_processes
        })
    
    def stop(self):
        """Stop the simulation"""
        self.running = False
        
        # Signal all processes to stop
        for process in self.processes:
            process.stop_flag.set()
        
        # Wait for processes with timeout
        for process in self.processes:
            process.join(timeout=2.0)
        
        self.sim_stats.end_time = time.time()
        
        self.stats_queue.put({
            'type': 'simulation_stopped',
            'stats': self.sim_stats
        })
    
    def reset(self):
        """Reset the simulation"""
        self.stop()
        self.rm = ResourceManager()
        self.processes = []
        self.sim_stats = SimulationStats()

# ============================================================================
# GUI APPLICATION - FIXED VERSION
# ============================================================================

class DeadlockDemoGUI:
    """Main GUI application"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("Deadlock Prevention Demonstration")
        self.root.geometry("1400x900")
        
        self.engine: Optional[SimulationEngine] = None
        self.update_task = None
        
        self.setup_ui()
        self.update_ui()
    
    def setup_ui(self):
        """Setup the user interface"""
        # Main container
        main_container = ttk.Frame(self.root, padding="10")
        main_container.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_container.columnconfigure(1, weight=1)
        main_container.rowconfigure(2, weight=1)
        
        # Control Panel
        self.setup_control_panel(main_container)
        
        # Visualization Panel
        self.setup_visualization_panel(main_container)
        
        # Log Panel
        self.setup_log_panel(main_container)
        
        # Statistics Panel
        self.setup_statistics_panel(main_container)
    
    def setup_control_panel(self, parent):
        """Setup control panel"""
        control_frame = ttk.LabelFrame(parent, text="Simulation Controls", padding="10")
        control_frame.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)
        
        # Strategy selection
        ttk.Label(control_frame, text="Prevention Strategy:").grid(row=0, column=0, sticky=tk.W, padx=5)
        self.strategy_var = tk.StringVar(value=PreventionStrategy.NONE.value)
        strategy_combo = ttk.Combobox(control_frame, textvariable=self.strategy_var,
                                     values=[s.value for s in PreventionStrategy],
                                     state='readonly', width=25)
        strategy_combo.grid(row=0, column=1, sticky=tk.W, padx=5)
        
        # Number of processes
        ttk.Label(control_frame, text="Number of Processes:").grid(row=0, column=2, sticky=tk.W, padx=5)
        self.num_processes_var = tk.IntVar(value=5)
        num_processes_spin = ttk.Spinbox(control_frame, from_=2, to=10,
                                        textvariable=self.num_processes_var, width=10)
        num_processes_spin.grid(row=0, column=3, sticky=tk.W, padx=5)
        
        # Buttons
        self.start_button = ttk.Button(control_frame, text="Start Simulation", command=self.start_simulation)
        self.start_button.grid(row=0, column=4, padx=5)
        
        self.stop_button = ttk.Button(control_frame, text="Stop Simulation", command=self.stop_simulation, state='disabled')
        self.stop_button.grid(row=0, column=5, padx=5)
        
        self.reset_button = ttk.Button(control_frame, text="Reset", command=self.reset_simulation)
        self.reset_button.grid(row=0, column=6, padx=5)
        
        # Status
        self.status_var = tk.StringVar(value="Ready")
        status_label = ttk.Label(control_frame, textvariable=self.status_var, foreground="blue")
        status_label.grid(row=0, column=7, sticky=tk.W, padx=10)
    
    def setup_visualization_panel(self, parent):
        """Setup visualization panel with graphs"""
        viz_frame = ttk.LabelFrame(parent, text="Resource Allocation Visualization", padding="10")
        viz_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)
        
        # Create matplotlib figure
        self.fig = Figure(figsize=(7, 6), dpi=100)
        
        # Wait-for graph
        self.ax_graph = self.fig.add_subplot(211)
        self.ax_graph.set_title("Wait-For Graph")
        self.ax_graph.axis('off')
        
        # Resource allocation matrix
        self.ax_matrix = self.fig.add_subplot(212)
        self.ax_matrix.set_title("Resource Allocation Matrix")
        self.ax_matrix.axis('off')
        
        self.fig.tight_layout()
        
        # Embed in tkinter
        self.canvas = FigureCanvasTkAgg(self.fig, master=viz_frame)
        self.canvas.draw()
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
    
    def setup_log_panel(self, parent):
        """Setup log panel"""
        log_frame = ttk.LabelFrame(parent, text="Activity Log", padding="10")
        log_frame.grid(row=1, column=1, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5, padx=5)
        
        # Scrolled text widget
        self.log_text = scrolledtext.ScrolledText(log_frame, height=20, width=60,
                                                  font=('Courier', 9))
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
        # Configure tags for colored output
        self.log_text.tag_config('INFO', foreground='black')
        self.log_text.tag_config('SUCCESS', foreground='green')
        self.log_text.tag_config('WARNING', foreground='orange')
        self.log_text.tag_config('ERROR', foreground='red')
    
    def setup_statistics_panel(self, parent):
        """Setup statistics panel"""
        stats_frame = ttk.LabelFrame(parent, text="Statistics", padding="10")
        stats_frame.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)
        
        # Create statistics labels
        self.stat_labels = {}
        stats_to_show = [
            ('Processes', 'processes'),
            ('Completed', 'completed'),
            ('Deadlocks', 'deadlocks'),
            ('Total Retries', 'retries'),
            ('Avg Wait Time', 'wait_time'),
            ('Throughput', 'throughput')
        ]
        
        for i, (label, key) in enumerate(stats_to_show):
            ttk.Label(stats_frame, text=f"{label}:").grid(row=0, column=i*2, sticky=tk.W, padx=5)
            var = tk.StringVar(value="0")
            self.stat_labels[key] = var
            ttk.Label(stats_frame, textvariable=var, foreground="blue", font=('Arial', 10, 'bold')).grid(row=0, column=i*2+1, sticky=tk.W, padx=5)
    
    def log_message(self, message: str, level: str = "INFO"):
        """Add message to log"""
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        formatted_message = f"[{timestamp}] {message}\n"
        
        self.log_text.insert(tk.END, formatted_message, level)
        self.log_text.see(tk.END)
        
        # Limit log size
        try:
            if int(self.log_text.index('end-1c').split('.')[0]) > 1000:
                self.log_text.delete('1.0', '500.0')
        except:
            pass
    
    def update_visualization(self):
        """Update the visualization graphs"""
        if not self.engine:
            return
        
        try:
            # Clear previous plots
            self.ax_graph.clear()
            self.ax_matrix.clear()
            
            # Get current allocation state
            state = self.engine.rm.get_allocation_state()
            
            # Draw wait-for graph
            self.draw_wait_for_graph(state)
            
            # Draw allocation matrix
            self.draw_allocation_matrix(state)
            
            self.canvas.draw()
        except Exception as e:
            # Silently handle visualization errors
            pass
    
    def draw_wait_for_graph(self, state):
        """Draw wait-for graph - FIXED VERSION"""
        try:
            G = nx.DiGraph()
            
            # Add nodes - use copies to avoid iteration issues
            allocated = state['allocated'].copy()
            waiting = state['waiting'].copy()
            
            process_ids = set()
            for holder in allocated.values():
                if holder is not None:
                    process_ids.add(holder)
            for pid in waiting.keys():
                process_ids.add(pid)
            
            for pid in process_ids:
                G.add_node(f"P{pid}", node_type='process')
            
            for resource in ResourceType:
                G.add_node(resource.name, node_type='resource')
            
            # Add edges
            # Process holds resource
            for resource, holder in allocated.items():
                if holder is not None:
                    G.add_edge(f"P{holder}", resource.name)
            
            # Process waits for resource
            for pid, waiting_resources in waiting.items():
                for resource in waiting_resources:
                    if resource in allocated:
                        holder = allocated[resource]
                        if holder is not None:
                            # Show waiting relationship
                            G.add_edge(resource.name, f"P{pid}", style='dashed', color='red')
            
            if len(G.nodes()) == 0:
                self.ax_graph.text(0.5, 0.5, 'No active processes', 
                                 ha='center', va='center', fontsize=12)
                self.ax_graph.set_title("Wait-For Graph")
                return
            
            # Layout
            process_nodes = [n for n, d in G.nodes(data=True) if d.get('node_type') == 'process']
            resource_nodes = [n for n, d in G.nodes(data=True) if d.get('node_type') == 'resource']
            
            pos = {}
            for i, node in enumerate(process_nodes):
                pos[node] = (0, i * 1.5)
            for i, node in enumerate(resource_nodes):
                pos[node] = (2, i * 1.2)
            
            # Draw
            if process_nodes:
                nx.draw_networkx_nodes(G, pos, nodelist=process_nodes,
                                      node_color='lightblue', node_size=800,
                                      node_shape='o', ax=self.ax_graph)
            if resource_nodes:
                nx.draw_networkx_nodes(G, pos, nodelist=resource_nodes,
                                      node_color='lightgreen', node_size=600,
                                      node_shape='s', ax=self.ax_graph)
            
            # Draw edges
            holds_edges = [(u, v) for u, v, d in G.edges(data=True) if d.get('style') != 'dashed']
            waits_edges = [(u, v) for u, v, d in G.edges(data=True) if d.get('style') == 'dashed']
            
            if holds_edges:
                nx.draw_networkx_edges(G, pos, edgelist=holds_edges,
                                      edge_color='black', arrows=True, ax=self.ax_graph,
                                      arrowsize=15)
            if waits_edges:
                nx.draw_networkx_edges(G, pos, edgelist=waits_edges,
                                      edge_color='red', style='dashed', arrows=True,
                                      ax=self.ax_graph, arrowsize=15, width=2)
            
            nx.draw_networkx_labels(G, pos, ax=self.ax_graph, font_size=9)
            
            # Check for deadlock
            try:
                deadlocked = self.engine.rm.detect_deadlock()
                if deadlocked:
                    title = "Wait-For Graph - ⚠️ DEADLOCK DETECTED!"
                    title_color = 'red'
                else:
                    title = "Wait-For Graph - ✓ No Deadlock"
                    title_color = 'green'
            except:
                title = "Wait-For Graph"
                title_color = 'black'
            
            self.ax_graph.set_title(title, fontsize=12, color=title_color, fontweight='bold')
            self.ax_graph.axis('off')
        except Exception as e:
            self.ax_graph.clear()
            self.ax_graph.text(0.5, 0.5, 'Visualization Error', 
                             ha='center', va='center', fontsize=12)
            self.ax_graph.axis('off')
    
    def draw_allocation_matrix(self, state):
        """Draw resource allocation matrix - FIXED VERSION"""
        try:
            # Get all process IDs - use copies
            allocated = state['allocated'].copy()
            waiting = state['waiting'].copy()
            
            process_ids = set()
            for holder in allocated.values():
                if holder is not None:
                    process_ids.add(holder)
            for pid in waiting.keys():
                process_ids.add(pid)
            
            if not process_ids:
                self.ax_matrix.text(0.5, 0.5, 'No active processes',
                                  ha='center', va='center', fontsize=12)
                self.ax_matrix.set_title("Resource Allocation Matrix")
                return
            
            process_ids = sorted(list(process_ids))
            resources = list(ResourceType)
            
            # Create matrix data
            matrix = []
            for pid in process_ids:
                row = []
                for resource in resources:
                    if resource in allocated and allocated[resource] == pid:
                        row.append('H')  # Holds
                    elif pid in waiting and resource in waiting[pid]:
                        row.append('W')  # Waits
                    else:
                        row.append('-')  # Not needed
                matrix.append(row)
            
            # Draw table
            table_data = [[''] + [r.name for r in resources]]  # Header
            for i, pid in enumerate(process_ids):
                table_data.append([f'P{pid}'] + matrix[i])
            
            table = self.ax_matrix.table(cellText=table_data, loc='center',
                                        cellLoc='center', bbox=[0, 0, 1, 1])
            table.auto_set_font_size(False)
            table.set_fontsize(10)
            table.scale(1, 2)
            
            # Color code cells
            for i in range(1, len(table_data)):
                for j in range(1, len(table_data[0])):
                    cell = table[(i, j)]
                    if table_data[i][j] == 'H':
                        cell.set_facecolor('lightgreen')
                    elif table_data[i][j] == 'W':
                        cell.set_facecolor('yellow')
                    else:
                        cell.set_facecolor('white')
            
            # Header styling
            for j in range(len(table_data[0])):
                table[(0, j)].set_facecolor('lightgray')
                table[(0, j)].set_text_props(weight='bold')
            
            for i in range(len(table_data)):
                table[(i, 0)].set_facecolor('lightgray')
                table[(i, 0)].set_text_props(weight='bold')
            
            self.ax_matrix.set_title("Resource Allocation Matrix\nH=Holds, W=Waits, -=Not Needed",
                                    fontsize=10)
            self.ax_matrix.axis('off')
        except Exception as e:
            self.ax_matrix.clear()
            self.ax_matrix.text(0.5, 0.5, 'Matrix Error', 
                              ha='center', va='center', fontsize=12)
            self.ax_matrix.axis('off')
    
    def start_simulation(self):
        """Start the simulation"""
        # Get selected strategy
        strategy_name = self.strategy_var.get()
        strategy = next(s for s in PreventionStrategy if s.value == strategy_name)
        
        num_processes = self.num_processes_var.get()
        
        # Create engine
        self.engine = SimulationEngine(strategy, num_processes)
        
        # Update UI
        self.start_button.config(state='disabled')
        self.stop_button.config(state='normal')
        self.status_var.set(f"Running: {strategy.value}")
        
        # Clear log
        self.log_text.delete('1.0', tk.END)
        self.log_message(f"Starting simulation with {strategy.value} strategy", "INFO")
        self.log_message(f"Number of processes: {num_processes}", "INFO")
        
        # Start simulation
        self.engine.start()
        
        # Start update loop
        self.update_ui()
    
    def stop_simulation(self):
        """Stop the simulation"""
        if self.engine:
            self.log_message("Stopping simulation...", "WARNING")
            self.engine.stop()
            self.status_var.set("Stopped")
            self.start_button.config(state='normal')
            self.stop_button.config(state='disabled')
    
    def reset_simulation(self):
        """Reset the simulation"""
        if self.engine:
            self.engine.reset()
        
        self.log_text.delete('1.0', tk.END)
        self.log_message("Simulation reset", "INFO")
        
        # Reset statistics
        for var in self.stat_labels.values():
            var.set("0")
        
        self.status_var.set("Ready")
        self.start_button.config(state='normal')
        self.stop_button.config(state='disabled')
        
        # Clear visualizations
        self.ax_graph.clear()
        self.ax_matrix.clear()
        self.canvas.draw()
    
    def update_ui(self):
        """Update UI with latest data"""
        if self.engine and self.engine.running:
            # Process messages from queue
            try:
                while True:
                    msg = self.engine.stats_queue.get_nowait()
                    self.process_message(msg)
            except queue.Empty:
                pass
            
            # Update visualizations
            self.update_visualization()
            
            # Update statistics
            self.update_statistics()
        
        # Schedule next update
        self.update_task = self.root.after(100, self.update_ui)
    
    def process_message(self, msg):
        """Process a message from the stats queue"""
        msg_type = msg.get('type')
        
        if msg_type == 'log':
            process_id = msg.get('process_id')
            message = msg.get('message')
            level = msg.get('level', 'INFO')
            self.log_message(f"P{process_id}: {message}", level)
        
        elif msg_type == 'simulation_started':
            strategy = msg.get('strategy')
            self.log_message(f"=== Simulation Started: {strategy} ===", "SUCCESS")
        
        elif msg_type == 'simulation_stopped':
            self.log_message("=== Simulation Stopped ===", "WARNING")
            stats = msg.get('stats')
            if stats:
                self.log_message(f"Total time: {stats.end_time - stats.start_time:.2f}s", "INFO")
        
        elif msg_type == 'process_complete':
            stats = msg.get('stats')
            if stats.state == ProcessState.COMPLETED:
                self.log_message(f"P{stats.process_id} completed in {stats.total_time:.2f}s", "SUCCESS")
                self.engine.sim_stats.completed_processes += 1
            else:
                self.log_message(f"P{stats.process_id} failed/timeout", "ERROR")
            
            self.engine.sim_stats.total_retries += stats.retry_count
        
        elif msg_type == 'deadlock_detected':
            processes = msg.get('processes', [])
            self.log_message(f"⚠️ DEADLOCK DETECTED! Processes involved: {processes}", "ERROR")
        
        elif msg_type == 'state_update':
            # Could add more detailed state tracking here
            pass
    
    def update_statistics(self):
        """Update statistics display"""
        if not self.engine:
            return
        
        try:
            stats = self.engine.sim_stats
            
            self.stat_labels['processes'].set(str(stats.total_processes))
            self.stat_labels['completed'].set(str(stats.completed_processes))
            self.stat_labels['deadlocks'].set(str(stats.deadlocks_detected))
            self.stat_labels['retries'].set(str(stats.total_retries))
            
            # Calculate average wait time from processes
            if self.engine.processes:
                total_wait = sum(p.stats.wait_time for p in self.engine.processes)
                avg_wait = total_wait / len(self.engine.processes)
                self.stat_labels['wait_time'].set(f"{avg_wait:.2f}s")
            
            # Calculate throughput
            if stats.end_time > stats.start_time:
                throughput = stats.completed_processes / (stats.end_time - stats.start_time)
                self.stat_labels['throughput'].set(f"{throughput:.2f}/s")
            elif self.engine.running and stats.start_time > 0:
                elapsed = time.time() - stats.start_time
                if elapsed > 0:
                    throughput = stats.completed_processes / elapsed
                    self.stat_labels['throughput'].set(f"{throughput:.2f}/s")
        except:
            pass

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """Main entry point"""
    root = tk.Tk()
    app = DeadlockDemoGUI(root)
    
    # Handle window close
    def on_closing():
        if app.engine:
            app.engine.stop()
        root.destroy()
    
    root.protocol("WM_DELETE_WINDOW", on_closing)
    root.mainloop()

if __name__ == "__main__":
    main()