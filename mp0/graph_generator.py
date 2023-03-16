import numpy as np
import matplotlib.pyplot as plt

# plotting the bandwidth vs time using bandwidth.txt
with open('bandwidth.txt', 'r') as file1:
    lines = file1.readlines()
    sec = 0
    Bandwidth = []
    for line in lines:
        sec += 1
        if sec > 100:
            break
        data = int(line)
        Bandwidth.append(data)

time = list(range(100))
fig1 = plt.figure()
ax1 = fig1.add_subplot(111)
ax1.plot(time, Bandwidth)
ax1.set_xlabel('Time')
ax1.set_ylabel('Bandwidth')
ax1.set_title('Bandwidth vs Time')

# plotting the min max avg 90th percentile delay vs time using time_logger.txt
with open('time_logger.txt', 'r') as file:
    lines = file.readlines()
    MINs = []
    MAXs = []
    AVGs = []
    PERs = []
    sec  = 0
    for line in lines:
        sec += 1
        if sec > 100:
            break
        data = line.split()
        mins = float(data[0])
        maxs = float(data[1])
        avgs = float(data[2])
        pers = float(data[3])
        MINs.append(mins)
        MAXs.append(maxs)
        AVGs.append(avgs)
        PERs.append(pers)
time = list(range(100))
fig2 = plt.figure()

ax2 = fig2.add_subplot(111)
ax2.plot(time, MINs)
ax2.set_xlabel('Time')
ax2.set_ylabel('Min Delay')
ax2.set_title('Min Delay vs Time')

fig3 = plt.figure()
ax3 = fig3.add_subplot(111)
ax3.plot(time, MAXs)
ax3.set_xlabel('Time')
ax3.set_ylabel('Max Delay')
ax3.set_title('Max Delay vs Time')

fig4 = plt.figure()
ax4 = fig4.add_subplot(111)
ax4.plot(time, AVGs)
ax4.set_xlabel('Time')
ax4.set_ylabel('AVG Delay')
ax4.set_title('Average Delay vs Time')

fig5 = plt.figure()
ax5 = fig5.add_subplot(111)
ax5.plot(time, PERs)
ax5.set_xlabel('Time')
ax5.set_ylabel('90th Percentile Delay')
ax5.set_title('90th Percentile Delay vs Time')
plt.show()




