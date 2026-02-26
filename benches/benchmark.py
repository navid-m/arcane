#!/usr/bin/env python3

"""
This is for benchmarking and testing the server, since there's no easy way to do that without mocks.
"""

import socket
import threading
import time
from statistics import mean, median

HOST = "127.0.0.1"
PORT = 7734


def send_command(cmd):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        s.sendall(f"{cmd}\n".encode())
        response = b""
        while True:
            chunk = s.recv(4096)
            if not chunk:
                break
            response += chunk
            if b"END\n" in response or b"ERR" in response:
                break
        return response.decode()


def benchmark_inserts(n, threads=1):
    def worker(start, end, results):
        times = []
        for i in range(start, end):
            t0 = time.perf_counter()
            send_command(f'insert into bench (id: {i}, data: "test_{i}")')
            times.append(time.perf_counter() - t0)
        send_command('commit!')
        results.extend(times)

    results = []
    workers = []
    chunk = n // threads

    start_time = time.perf_counter()
    for t in range(threads):
        start = t * chunk
        end = (t + 1) * chunk if t < threads - 1 else n
        w = threading.Thread(target=worker, args=(start, end, results))
        w.start()
        workers.append(w)

    for w in workers:
        w.join()

    total_time = time.perf_counter() - start_time
    return total_time, results


def main():
    print("Setting up...")
    send_command("create bucket bench (id: int, data: string)")

    configs = [
        (1000, 1, "1K inserts, 1 thread"),
        (1000, 4, "1K inserts, 4 threads"),
        (5000, 8, "5K inserts, 8 threads"),
    ]

    for n, threads, desc in configs:
        print(f"\n{desc}:")
        total, times = benchmark_inserts(n, threads)
        print(f"  Total: {total:.2f}s")
        print(f"  Throughput: {n / total:.0f} ops/sec")
        print(
            f"  Latency - avg: {mean(times) * 1000:.2f}ms, median: {median(times) * 1000:.2f}ms"
        )

    print("\nQuerying...")
    t0 = time.perf_counter()
    result = send_command("get * from bench")
    query_time = time.perf_counter() - t0
    rows = result.count("\n") - 4
    print(f"  Scanned {rows} rows in {query_time:.3f}s")


if __name__ == "__main__":
    main()
