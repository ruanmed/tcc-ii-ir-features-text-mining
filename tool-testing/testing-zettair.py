import subprocess
import time


cmd_index_moby = '''zet -i -f moby_index -t TREC moby-reduced.txt'''

cmd = '''zet --okapi --k1=1.2 --b=0.75 --summary=none -n 20 --big-and-fast whale '''
cmd2 = '''zet -f moby_index --okapi --k1=1.2 --b=0.75 --summary=none -n 20 --big-and-fast'''
query = 'sea whale'

start = time.time()
results = subprocess.run(
    cmd_index_moby, shell=True, universal_newlines=True, check=True, capture_output=True)
# b = subprocess.Popen(['zet', '--index',  '--filename', '-t', 'TREC', 'moby_index', '--big-and-fast', 'moby-reduced.txt'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
a = subprocess.Popen(['zet',  '-f', 'moby_index', '--okapi', '--k1=1.2', '--b=0.75', '--summary=none', '--big-and-fast'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
out, err = a.communicate(query.encode('ascii'))
end = time.time()
print("A TEST")
# print(results.stdout)
print(out.decode('utf-8'))
print(err)
print(end - start)
