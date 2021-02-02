# Source
I developed this flink project as an assignment of 'Cloud Computing' course instructed by Ainhoa Azqueta at Universidad Politécnica de Madrid(UPM). You can find the description of the assignment in a file called 'FileProject20-21.pdf' in the repository. The original source of dataset and description belongs to Arvind Arasu et al. You may find the article [here](https://www.cs.brandeis.edu/~anurag/publications/linearroad-04.pdf). 

# Project Description
This project simulates a toll system for the expressways of a large metropolitan area. The city is 100 miles wide and 100 miles long and is divided into a grid such that the origin is the southwestern most point in the city, and coordinate (x, y) is x feet east and y feet north of the origin. In the metropolitan area there are 10 parallel expressways (0-9) and all of them are 100 miles long, as represented in Figure 2. All expressways are 527999 feet long (100 miles) and each expressway has 5 lanes (0-4) in each direction (east and west). Each expressway has 100 entrance ramps and 100 exit ramps in each direction, dividing it into 1 milelong segments. Each vehicle is equipped with a sensor that reports the car position every 30 seconds each time the vehicle is in one of the expressways. The position is represented by (Time, VID, Spd, XWay, Lane, Dir, Seg, Pos), being Time a timestamp identifying the time at which the position report was emitted; VID is an integer identifying the vehicle. Spd is an integer (0...100) representing the speed of the vehicle; Xway is the expressway identifier (0…9) and Lane indicates the expressway lane (0…4). Dir indicates de direction (0 eastbound and 1 westbound), Seg (0…99) is the mile-long segment from which the position report is emitted and Pos (0…527999) is the horizontal position of the vehicle as a measure of the number of feet from westernmost point on the expressway, respectively. Strictly speaking Seg is redundant given that position reports include Pos. An example of the positions emitted is:
```
0,107,32,0,0,0,10,53320
0,109,20,0,0,0,19,100644
0,106,28,0,0,0,26,137745
0,108,32,0,0,0,67,354281
```

'Exercise 3' has two goals: 1) Calculate the average speed of the cars in a given segment. More concretely, the output events are tuples with (VID, xway, average speed of the car).
2) Spot the cars with the highest average speed in a given segment every hour. The information to be output is (VID, xway and average speed of the vehicle with the highest average speed). 

# Software Requirements
Oracle Java 8, Flink 1.9.1.

# How to Run
You may modify the segment number and the output file name.
```
flink run -c es.upm.master.exercise3 LinearRoadProject-1.0-SNAPSHOT.jar -input vehiclesData_top1000.csv -output1 exercise3-1.csv -output2 exercise3-2.csv -segment 47
```
