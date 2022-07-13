import time
import itertools
import subprocess
import tempfile
from concurrent import futures
import os
import shutil
from pathlib import Path

# for cli imports
import argparse, sys

from rich.progress import Progress
from rich import progress as rp
from rich.progress import track

# repeattest 
# Script to run tests multiple times in parallel and aggregate results across them

# concurrent.futures requires tasks that are encapsulated in a function
def run_test(test: str, race: bool):
    test_cmd = ["go", "test", f"-run={test}"]
    if race:
        test_cmd.append("-race")
    f, path = tempfile.mkstemp()
    start = time.time()
    proc = subprocess.run(test_cmd, stdout=f, stderr=f)
    runtime = time.time() - start
    os.close(f)
    return test, path, proc.returncode, runtime

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--test_names', nargs='+')
    parser.add_argument('--iterations', nargs='?', type=int, default = 1)
    parser.add_argument('--workers', nargs='?', type=int, default = 10)
    parser.add_argument('--race', action=argparse.BooleanOptionalAction)
    parser.add_argument('--savepasses', action=argparse.BooleanOptionalAction)
    parser.add_argument('--output_path', nargs='?', default = './test/foo')
    
    args = parser.parse_args()
    test_names = args.test_names
    iterations = args.iterations
    savepasses = bool(args.savepasses)
    workers = args.workers
    race = bool(args.race)
    output = Path(args.output_path)
    print(output)

    progress_bars = dict()
    total = iterations * len(test_names)
    pass_count = dict.fromkeys(test_names, 0)
    completion_counts = dict.fromkeys(test_names, 0)
    failure_count = dict.fromkeys(test_names, 0)    

    # This will collate tests, so they run in a balanced way
    tests = itertools.chain.from_iterable(itertools.repeat(test_names, iterations))
    completed = 0

    with Progress(*Progress.get_default_columns(),rp.MofNCompleteColumn(), rp.TextColumn("{task.fields[failures]}")) as progress:       
        for k in test_names:
            progress_bars[k] = progress.add_task("[red]"+k,total = iterations, failures = 0) 

        with futures.ThreadPoolExecutor(max_workers=workers) as executor:

            futurelist = []
            while completed < total:
                # print("Running again")
                n = len(futurelist)
                # If there are fewer futures than workers assign them
                if n < workers:
                    for test in itertools.islice(tests, workers-n):
                        futurelist.append(executor.submit(run_test, test, race))

                # print(futurelist)

                # Wait until a task completes
                done, not_done = futures.wait(futurelist, return_when=futures.FIRST_COMPLETED)
                # print(done)
                # print(not_done)

                # print(results)

                for future in done:
                    test, path, rc, runtime = future.result()

                    completion_counts[test] += 1
                    pass_count[test] += 1 if rc == 0 else 0

                    dest = (output / f"{test}_{completed}.log").as_posix()
                    # If the test failed, save the output for later analysis
                    if savepasses or rc != 0:
                        output.mkdir(exist_ok=True, parents=True)
                        shutil.copy(path, dest)

                    if rc != 0:
                        print(f"Failed test {test} - {dest}")
                        failure_count[test] += 1

                    os.remove(path)
                    completed += 1
                    futurelist = list(not_done)

                for k,v in progress_bars.items():
                    progress.update(v, completed = completion_counts[k], failures = failure_count[k])
