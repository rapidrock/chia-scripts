#!/usr/bin/env python3
"""
Seed & Plant.

An efficient Chia plot creator and mover.

- Tested on Ubuntu 22.04 LTS. 
- Runs with Python 3.10+ and rsync installed.
- Start this script in the same directory as the bladebit_cuda executable.

Version: 2.1.0

Author: Matt <matt@rapidrock.io>
Inspired by Plow by author: Luke Macken <phorex@protonmail.com>
SPDX-License-Identifier: GPL-3.0-or-later
"""
import shutil
import random
import asyncio
from typing import Set
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Local plot sources
# For wildcards:
#   SOURCES = glob.glob('/mnt/*')
SOURCES = ["/mnt/Plotter_2TB"]

# Rsync destinations
# Examples: ["/mnt/HDD1", "192.168.1.10::hdd1"]
DESTS = [ ]

# Shuffle plot destinations. Useful when using many plotters to decrease the odds
# of them copying to the same drive simultaneously.
SHUFFLE = True 

# Rsync bandwidth limiting
BWLIMIT = None

# Optionally set the I/O scheduling class and priority
IONICE = "-c 3"  # "-c 3" for "idle", "-c 2" for "best-effort"

# Short & long sleep durations upon various error conditions
SLEEP_FOR = 60 * 3
SLEEP_FOR_LONG = 60 * 20

RSYNC_CMD = "rsync"

if SHUFFLE:
    random.shuffle(DESTS)

# Rsync parameters. For FAT/NTFS you may need to remove --preallocate
if BWLIMIT:
    RSYNC_FLAGS = f"--remove-source-files --preallocate --whole-file --bwlimit={BWLIMIT}"
else:
    RSYNC_FLAGS = "--remove-source-files --preallocate --whole-file --progress -h"

if IONICE:
    RSYNC_CMD = f"ionice {IONICE} {RSYNC_CMD}"
 
# Bladebit_Cuda parameters
BLADEBIT_CMD = "./bladebit_cuda"
PLOT_COUNT = 20
COMPRESS_LEVEL = 7
FARM_KEY = "<FARM_KEY>"
CONTRACT_KEY = "<CONTRACT_KEY>"
BLADEBIT_DEST = "/mnt/Plotter_2TB"

@dataclass
class PlotStats:
    total_plots_moved: int = 0
    total_plots_created: int = 0
    total_bytes_moved: int = 0
    start_time: datetime = datetime.now()

    def log_stats(self):
        runtime = datetime.now() - self.start_time
        days = runtime.days
        hours = runtime.seconds // 3600
        minutes = (runtime.seconds % 3600) // 60
        seconds = runtime.seconds % 60
        
        runtime_str = f"{days}d {hours}h {minutes}m {seconds}s"
        avg_speed = self.total_bytes_moved / runtime.total_seconds() if runtime.total_seconds() > 0 else 0
        total_gb = self.total_bytes_moved / (1024**3)
        
        print(f"\n📊 Stats:")
        print(f"   Plots created: {self.total_plots_created}")
        print(f"   Plots moved: {self.total_plots_moved}")
        print(f"   Runtime: {runtime_str}")
        print(f"   Total data moved: {total_gb:.2f} GB")
        print(f"   Average transfer speed: {avg_speed/1024/1024:.2f} MB/s")

class PlotManager:
    def __init__(self):
        self.stats = PlotStats()
        self.plot_queue = asyncio.Queue()
        self.plotting = False
        self.moving = False
        self.all_dests_full = False
        
    async def check_plotting_drive_space(self):
        """Check if there's enough space on plotting drive for another batch"""
        try:
            if not Path(BLADEBIT_DEST).exists() or not Path(BLADEBIT_DEST).is_mount():
                print(f"⚠️ Plotting destination {BLADEBIT_DEST} is not mounted")
                return False
            
            free_space = shutil.disk_usage(BLADEBIT_DEST).free
            plot_size = 84_000_000_000  # Approximate plot size in bytes
            return free_space >= plot_size * PLOT_COUNT
        except Exception as e:
            print(f"Error checking plotting drive space: {e}")
            return False

    async def check_dest_space(self, dest, plot_size):
        """Check if destination has space for one plot"""
        try:
            dest_path = Path(dest)
            if dest_path.exists() and dest_path.is_mount():
                free_space = shutil.disk_usage(dest).free
                return free_space >= plot_size
        except Exception as e:
            print(f"Error checking space on {dest}: {e}")
        return False

    async def create_plots(self):
        """Create a batch of plots"""
        print(f"🌱 Creating {PLOT_COUNT} plots on {BLADEBIT_DEST}")
        
        cmd = f"{BLADEBIT_CMD} -f {FARM_KEY} -c {CONTRACT_KEY} -n {PLOT_COUNT} --compress {COMPRESS_LEVEL} cudaplot {BLADEBIT_DEST}"
        
        try:
            start = datetime.now()
            proc = await asyncio.create_subprocess_shell(cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await proc.communicate()
            
            if proc.returncode == 0:
                self.stats.total_plots_created += PLOT_COUNT
                print(f"✅ Plot creation completed in {datetime.now() - start}")
                return True
            else:
                print(f"❌ Plot creation failed with code {proc.returncode}")
                if stderr:
                    print(f"Error: {stderr.decode()}")
                return False
                
        except Exception as e:
            print(f"❌ Plot creation error: {e}")
            return False

    async def scan_for_plots(self):
        """Scan plotting directory for plots and add to queue"""
        plots = list(Path(BLADEBIT_DEST).glob("**/*.plot"))
        for plot in plots:
            await self.plot_queue.put(plot)
        return len(plots)

    async def move_plots(self):
        """Move plots from queue to destinations concurrently - one per destination"""
        
        async def move_to_dest(dest, plots_in_transit):
            while True:
                if self.plot_queue.empty():
                    break
     
                plot = await self.plot_queue.get()
                
                if not plot.exists():
                    self.plot_queue.task_done()
                    continue

                plot_size = plot.stat().st_size
                
                # Check destination before transfer
                if not await test_destination(dest):
                    print(f"⚠️ Destination {dest} not accessible, skipping...")
                    # Put plot back in queue for other destinations
                    await self.plot_queue.put(plot)
                    self.plot_queue.task_done()
                    break
                    
                if not await self.check_dest_space(dest, plot_size):
                    print(f"📦 Destination {dest} is full, removing from rotation")
                    # Put plot back in queue for other destinations
                    await self.plot_queue.put(plot)
                    self.plot_queue.task_done()
                    break
                    
                plots_in_transit.add(plot)
                if await transfer_plot(plot, dest):
                    self.stats.total_plots_moved += 1
                    self.stats.total_bytes_moved += plot_size
                plots_in_transit.remove(plot)
                self.plot_queue.task_done()

        # Track plots currently being moved to avoid duplicates
        plots_in_transit: Set[Path] = set()
        
        # Create tasks for each destination
        tasks = []
        for dest in DESTS:
            task = asyncio.create_task(move_to_dest(dest, plots_in_transit))
            tasks.append(task)
        
        # Wait for all transfers to complete
        await asyncio.gather(*tasks)
        
        # Check if we've run out of space everywhere
        if not self.plot_queue.empty():
            remaining = self.plot_queue.qsize()
            print(f"⚠️ {remaining} plots remaining but no destinations have space")
            self.all_dests_full = True

    async def run(self):
        """Main loop alternating between creating and moving plots"""
        print(f"🚜 Starting Seed & Plant at {datetime.now()}")
        
        while not self.all_dests_full:
            try:
                # First check for any plots to move
                existing_plots = await self.scan_for_plots()
                if existing_plots > 0:
                    print(f"Found {existing_plots} existing plots to move")
                    await self.move_plots()
                    if self.all_dests_full:
                        print("All destinations are full")
                        break
                    continue  # Go back to checking for plots before plotting
                
                # No plots to move, try creating some
                if await self.check_plotting_drive_space():
                    self.plotting = True
                    try:
                        if not await self.create_plots():
                            print("Plot creation failed, waiting before retry...")
                            await asyncio.sleep(60)
                            continue
                    finally:
                        self.plotting = False
                    
                    # Add small delay between cycles
                    await asyncio.sleep(5)
                    continue  # Go back to checking for plots
                else:
                    print("Not enough space for plotting, waiting...")
                    await asyncio.sleep(60)  # Wait before checking again
                    
            except Exception as e:
                print(f"Error in main loop: {e}")
                await asyncio.sleep(60)  # Wait before retrying
            
        self.stats.log_stats()

    async def cleanup(self):
        """Cleanup tasks before exit"""
        if self.plotting:
            print("Waiting for current plotting to complete...")
            # Could add plot process termination here if needed
        
        if not self.plot_queue.empty():
            print(f"Warning: {self.plot_queue.qsize()} plots remaining in queue")

async def transfer_plot(plot: Path, dest: str) -> bool:
    
    plot_size = plot.stat().st_size

    cmd = f"{RSYNC_CMD} {RSYNC_FLAGS} {plot} {dest}"

    try:
        print(f"🚜 {plot} ➡️ {dest}")
        
        # Now rsync the real plot
        proc = await asyncio.create_subprocess_shell(
            cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        start = datetime.now()
        stdout, stderr = await proc.communicate()
        finish = datetime.now()

        # Rsync print output
        if stdout:
            output = stdout.decode().strip()
            if output:
                print(f"Rsync: {output}")
        if stderr:
            print(f"⁉️ {stderr.decode()}")

        if proc.returncode == 0:
            duration = finish - start
            speed = plot_size / duration.total_seconds() / 1024 / 1024  # MB/s
            print(f"🏁 Transfer complete: {speed:.2f} MB/s ({duration})")
            return True
        elif proc.returncode == 10:  # Error in socket I/O
            # Retry later.
            print(f"⁉️ {cmd!r} exited with {proc.returncode} (error in socket I/O)")
            return False
        elif proc.returncode in (11, 23):  # Error in file I/O
            # Most likely a full drive.
            print(f"⁉️ {cmd!r} exited with {proc.returncode} (error in file I/O)")
            print(f"{dest} planting stopping")
            return False
        else:
            print(f"⁉️ {cmd!r} exited with {proc.returncode}")
            await asyncio.sleep(SLEEP_FOR)
            print(f"{dest} planting stopping")
            return False
        
    except Exception as e:
        print(f"! Error transferring plot: {e}")
        print(f"🚨 {cmd!r} exited with {proc.returncode}")
        return False
    
async def test_destination(dest: str) -> bool:
    test_cmd = f"rsync /etc/hostname {dest}"
    try:
        proc = await asyncio.create_subprocess_shell(
            test_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        await proc.communicate()
        if proc.returncode != 0:
            print(f"⁉️  {test_cmd!r} exited with {proc.returncode}")

        return proc.returncode == 0
    except Exception as e:
        print(f"⁉️ Destination connection test failed: {e}")
        print(f"⁉️  {test_cmd!r} exited with {proc.returncode}")
        return False

if __name__ == "__main__":
    manager = PlotManager()
    try:
        asyncio.run(manager.run())
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
        asyncio.run(manager.cleanup())
        manager.stats.log_stats()
