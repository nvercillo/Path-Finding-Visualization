# Path-Finding Visualization

## Prod Model
Visit [DEMO SITE](https://stefanvercillo.com/path-finding-visualization) to see a working production model of this project

## Introduction

The purpose of this project is to provide a visualziation of a path finding algorthim, finding the shortest path in terms of distance between two non-adjacent airports. To do this, a modified A* greedy algorthim was developed to work in a way that takes into account user-experience with a fast compute time. Using this heuristic guilded method, the best path is found and visualized in real time using JavaScript animations created to optimize performance with concurrent programming.

Using open source data from open2flights a list of every active airport and airport route in the world is store. This is based off an importaant pathfinding algorithm used in solving numerous computer science problems so it was a great experience to build this, I hope you enjoy!

## Features

  
#### Enter desired route and watch visualization. See how good the algorithm did comparing the computed path length and the length of the direct connection! 
  ![](/static/images/map.PNG)


## Stack
This project was created in Javascript backed by a Python Flask application. All of the dependencies are saved in a python virtual environment in this repository, so setup and instillation is fairly straight forward


## Setup
Here is how to get started with running the project:

### Installations
Have python, pip and virtual environment installed

### Configuration
This is handled in the project for you

### Running the Project
Simply run the batch script startServer.bat and everything will be handled by that!

### Acnowledgements
* AmCharts model was used to help create globe and a simple filight animation. 
* OpenFlights2 provided a DB which was used to help create this project
* Training, CV, and test sets all extracted randomly using the 10000 most common words on Wikipedia per language

