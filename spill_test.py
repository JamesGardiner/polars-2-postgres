import os, time, threading, shutil, polars as pl
peak = {'tmp': 0}
def watch():
    while True:
        tot = 0
        for root, _, files in os.walk(os.environ.get('POLARS_OOC_SPILL_DIR', '/tmp')):
            for f in files:
                try: tot += os.path.getsize(os.path.join(root, f))
                except OSError: pass
        peak['tmp'] = max(peak['tmp'], tot); time.sleep(0.5)
threading.Thread(target=watch, daemon=True).start()
t = time.perf_counter()
pl.scan_parquet('/data/events/*.parquet').sort('text', nulls_last=True).sink_parquet('/data/sorted.parquet')
print(f'sorted in {time.perf_counter()-t:.1f}s, peak spill on disk: {peak["tmp"]/1e6:.0f} MB')
mem = open('/sys/fs/cgroup/memory.peak').read().strip() if os.path.exists('/sys/fs/cgroup/memory.peak') else '?'
print(f'cgroup peak memory: {int(mem)/1e6:.0f} MB' if mem != '?' else 'no cgroup peak')
