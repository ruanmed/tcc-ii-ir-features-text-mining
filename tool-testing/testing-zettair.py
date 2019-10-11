import subprocess
import time

cmd = '''zet --okapi --k1=1.2 --b=0.75 --summary=none -n 20 --big-and-fast whale '''
cmd2 = '''zet --okapi --k1=1.2 --b=0.75 --summary=none -n 20 --big-and-fast'''
query = 'whale'

start = time.time()
# results = subprocess.run(
#     cmd2, shell=True, universal_newlines=True, check=True, capture_output=True)
a = subprocess.Popen(['zet', '--okapi', '--k1=1.2', '--b=0.75', '--summary=none', '--big-and-fast'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
result = a.communicate(query.encode('utf-8'))
end = time.time()
print("A TEST")
# print(results.stdout)
print(result)
print(end - start)
