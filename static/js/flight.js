// Classes for AStar Algorithm
class GraphNode { 
    constructor(title, parentName, absDistanceFrom, heuristic, connectionsAway){ 
        this.title = title;
        this.parent = parentName;
        this.visited = false;
        this.heapInd = -1;
        this.distance = absDistanceFrom;
        this.heuristic = heuristic;
        this.priority = absDistanceFrom + heuristic;
        // connections away 
        this.connectionsAway = connectionsAway;
    }
    visit() {
        this.visited = true;
    }
    update(newDistance){
        this.priority = newDistance + heuristic; 
    }
}
// heuristic guilded dijkstra's
class PriorityQueue {
    constructor(){
        this.minheap =[]; 
    }    

    left(ind){
        return 2 * ind+1;
    }

    right(ind) {
        return 2*ind +2;
    }

    parent(ind){
        var floor = Math.floor((ind-1)/2);
        return floor < 0 ? 0 : floor;
    }

    add(node){
        this.minheap.push(node);
        this.minheap[this.minheap.length-1].heapInd = this.minheap.length-1;
        this.bubbleUp();
    }

    updateNode(index, dist, parent){
        // if (dist >= this.minheap[index].distance) return;
        this.minheap[index].priority = dist + this.minheap[index].heuristic;
        this.minheap[index].distance = dist;
        this.minheap[index].parent = parent
        this.bubbleUp(index);
    }

    bubbleUp(index) {
        if (index == null) 
            index = this.minheap.length-1;
        while(index > 0){
            let node = this.minheap[index];
            let parentIndex = this.parent(index);
            let parentNode = this.minheap[parentIndex];

            if (parentNode.priority <= node.priority) break;
            this.minheap[index] = parentNode;
            this.minheap[parentIndex] = node;
            
            // sort out heap indecies
            this.minheap[index].heapInd = index;
            this.minheap[parentIndex].heapInd = parentIndex;
            index = parentIndex;
        }
    }

    pop(){
        return this.poll();
    }

    poll(){
        if (this.minheap.length <= 1 ) return this.minheap.pop();
        const min = this.minheap[0];
        this.minheap[0] = this.minheap.pop();
        // reassign heapInd 
        this.minheap[0].heapInd =0; 

        var i =0; 
        while(true){
            if (this.left(i)< this.minheap.length) {
                if (this.right(i) < this.minheap.length){
                    const lowest  = this.minheap[this.left(i)].priority >=  this.minheap[this.right(i)].priority ? this.right(i) : this.left(i);
                    if (this.minheap[i].priority <= this.minheap[lowest].priority) break;
                    const temp = this.minheap[i];
                    this.minheap[i] = this.minheap[lowest];
                    this.minheap[lowest] = temp;
                    
                    // heap Inds
                    this.minheap[i].heapInd = i;
                    this.minheap[lowest].heapInd = lowest; 
                    i = lowest; 
                } else {
                    if (this.minheap[this.left(i)].priority < this.minheap[i].priority){
                        const temp = this.minheap[i];
                        this.minheap[i] = this.minheap[this.left(i)];
                        this.minheap[this.left(i)] = temp; 

                        // heap Inds
                        this.minheap[i].heapInd = i;
                        this.minheap[this.left(i)].heapInd = this.left(i)
                    }
                    // end of array anyways
                    break;
                 }
            } else {
                break;
            }
        }
        return min;
    }
    
    isEmpty(){
        return this.minheap.length == 0 ? true : false;
    }

    peek (){
        return this.minheap[0];
    }

    searchNode(nodeName){
        for(var i =0; i<this.minheap.length; i++){
            if (this.minheap[i].title == nodeName){
                return this.minheap[i];
            }
       }        
       return -1;
    }
}

// ListNode Class
var ListNode = function (contents, nextNode, prevNode) { 
    this.data = contents; 
    this.next = nextNode;
    this.prev = prevNode; 
}

class List {
    constructor(){
        this.head = null;
        this.tail = null; 
        this.d = [];
    }

    
    enqueue_back (data) {
        var node = new ListNode(data, null, null);
        if (this.head == null){
            this.head = node; 
            this.tail = node;
            this.head.next = null;
            this.head.prev = null;
            this.tail.next = null;
            this.tail.prev = null;
            // one node
        } else if (this.head.next  == null){
            // add another node
            this.tail.prev = node;
            this.tail.next = null;

            this.head = node;
            this.head.next = this.tail;
            this.head.prev = null
        // } else if (this.head.next.next == null){
        } else {
            var oldHead = this.head;
            oldHead.prev = node;
            node.next = oldHead;
            this.head = node;
        }
    }

    push (node) {
        this.enqueue_back(node);
    }

    dequeue_front () {
        if (this.tail == null){
            return;
        } else if (this.tail.prev == null){
            this.tail = null;
            this.head = null;
        } else {
            this.tail = this.tail.prev;
            this.tail.next = null;
        }
    }

    pop () {
        this.dequeue_front();
    }

    print () {
        var t = this.head;
        while (t != null) {
            // console.log(t);
            t = t.next;
        }
    }

    peek () {
        return this.tail;
    }
}


//   API calls / page initiation scripts
async function start(from, to ) {
    var result = {};
    
    try {
        result = await $.ajax({
            url: "/getFlightData"
        });
    } catch (error) {
        // console.error(error);
    }
    
    
    // launch app
    var launcher = new Launcher(result);
    launcher.launch(from, to);
}




class Launcher {

    constructor (data){
        this.data = data;
        this.MAXVISITS = 0;
    }
    

    async launch(from, to){
        
        
        var ACTQUEUE = []
        var PATH = null;
        var TOTALD = null;
        var data = this.data
        var MAXVISITS = 0;
        var absDistance = 0;
        // console.log(data);


        //  INITAL VARIABLES
        var colors = [];
        var colors2= [];
        var startingCols = [];
        var yellowCols = [];
        // var DIVISOR = 100;
        var DIVISOR = 50;
    
        fillColorsArray(); // fills colors
        
        // Themes begin
        am4core.useTheme(am4themes_animated); // animated but tooltip colors are fucked up 
        // Themes end
        ``
        // Create map instance
        // console.log("SDFSDFSDFSD")
        var chart = am4core.create("chartdiv", am4maps.MapChart);
        chart.geodata = am4geodata_worldLow;
        // chart.projection = new am4maps.projections.Miller();           
        chart.projection = new am4maps.projections.Eckert6();           
        chart.homeZoomLevel = 2.5;
        chart.homeGeoPoint = {
            latitude: 38,
            longitude: 60
        };  
    

        var polygonSeries = chart.series.push(new am4maps.MapPolygonSeries());

        // Create map polygon series
        var polygonSeries = chart.series.push(new am4maps.MapPolygonSeries());
        polygonSeries.useGeodata = true;
        polygonSeries.mapPolygons.template.fill = chart.colors.getIndex(0).lighten(0.5);
        polygonSeries.mapPolygons.template.nonScalingStroke = true;
        polygonSeries.exclude = ["AQ"]; // exculde antartica

        // Configure series
        var polygonTemplate = polygonSeries.mapPolygons.template;
        polygonTemplate.tooltipText = "{name}";
        // polygonTemplate.fill = am4core.color("#d7e7ce");
        // polygonTemplate.stroke = am4core.color("#aaa");
        // polygonTemplate.stroke = am4core.color("#000");
        polygonTemplate.strokeWidth = 1

        // Create hover state and set alternative fill color
        // var hs = polygonTemplate.states.create("hover");
        // hs.properties.fill = am4core.color("green");

        // Add grid
        var grid = chart.series.push(new am4maps.GraticuleSeries());
        grid.toBack();
    
        // Add line bullets
        var cities = chart.series.push(new am4maps.MapImageSeries());
        cities.mapImages.template.nonScaling = true;
    
        var city = cities.mapImages.template.createChild(am4core.Circle);
        city.radius = 3;
        city.fill = chart.colors.getIndex(0).brighten(-0.2);
        // city.fill = "rgb(144,12,63)"
        city.strokeWidth = 1;
        city.stroke = am4core.color("#fff");
    
        var startAirport = "";
        var endAirport = ""; 

        function setStartAndEnd(start, end){
            startAirport = start;
            endAirport = end; 
        }

        setStartAndEnd("Toronto", 'Los Angeles');
    
        var cityObjects = {};
        var sizes = {};
        var planeAnimationTime = 0;
        function setSizes(start, end){  
            sizes = {'start': start, 'end' : end}
        }

        function setPlaneAnimationTime (ms){
            planeAnimationTime = ms;
        }

        // setPlaneAnimationTime(300);
        setPlaneAnimationTime(1500);
        setSizes(3, 8);


        
        // fly('CDG', 'IST')
        // // console.log(aStar('AER', 'YYZ'));
        animateAStar(from, to);
        async function animateAStar(from, to){
            aStar(from, to);
            
            
            if (PATH != 'path not found'){
                console.log(PATH)
                console.log(ACTQUEUE)



                var text = "Most Optimal Path: "

                for(var i= PATH.length-1; i>=0; i--){
                    text += PATH[i] 
                    if (i != 0){
                        text += " -> "
                    }
                }


                var para = document.createElement("h6");
                var node = document.createTextNode(text);
                para.appendChild(node);
                var element = document.getElementById("preview");
                element.appendChild(para);
                
                text = ""
                text += "\nNumber of Connections: "
                text += PATH.length -1;

                var para2 = document.createElement("h6");
                node = document.createTextNode(text);
                para2.appendChild(node);
                element = document.getElementById("preview");
                element.appendChild(para2);

                text = ""
                text += "Distance of Path: "
                text += Math.round(TOTALD/1000) + " km"
                
                var para3 = document.createElement("h6");
                node = document.createTextNode(text);
                para3.appendChild(node);
                element = document.getElementById("preview");
                element.appendChild(para3);


                text = ""
                text = "Direct Distance from " + PATH[PATH.length -1] + " to " + PATH[0] + ": " + Math.round(absDistance /1000) + " km"

                var para3 = document.createElement("h6");
                node = document.createTextNode(text);
                para3.appendChild(node);
                element = document.getElementById("preview");
                element.appendChild(para3);
                
                var para0 = document.createElement("h6");
                element = document.getElementById("preview");
                node = document.createTextNode(".");
                element.appendChild(para0);
                


                await animateMap(animateCityCreation);
                
                // setTimeout(async  function() {
                // fly("AER", "MSQ", 0);    
                //     // fly("AER", "KZN", 0);
                //     // addLine("AER", "MSQ")
                //     // addLine("AER", "KZN")
                // }, 5000)
            }
            
        }

        
        var FROM = null;
        async function aStar(from, to){       // to and from are strings ex. "YYZ", "LAX"
            FROM = from;
            const iterationLimit = 100000;
            var iteration = 0;
            var pq = new PriorityQueue();

            let gn = new GraphNode(from, null, 0, 0, 0);
            var heuristicCoords = {};
            var nodeMap = {};
            gn.visit();
            nodeMap[from] = gn;
            pq.add(gn);
            
            while (!pq.isEmpty() && iteration < iterationLimit){
                var top = pq.poll();
                
                ACTQUEUE.push({'action': 'delay', 'value': 500});
                
                if (top.connectionsAway+ 1 > MAXVISITS){
                    MAXVISITS = top.connectionsAway +1;
                }
                // animate city visit
                ACTQUEUE.push({'action': 'animatecity', 'city': top.title, 'type' : 2, 'iteration': top.connectionsAway+1});
                // ACTQUEUE.push({'action': 'animatecity', 'city': "YYZ", 'type' : 4});
                
                /* Should we delay here? */
                // ACTQUEUE.push({'action': 'delay', 'value': 1000});
                
                // add city
                top.visit();
                // console.log(top);

                var flyToList = [];
                for (var i =0; i<data['routes'][top.title].length; i++){

                    let neighbor = data['routes'][top.title][i];
                    var inNodeMap = false;

                    if (neighbor in nodeMap){
                        if (nodeMap[neighbor].visited){
                            continue;
                        }
                        inNodeMap = true;
                    }

                    // add new cities 
                    ACTQUEUE.push({'action': 'animatecity', 'city': neighbor, 'type' : -1});
                    
                    flyToList.push([top, neighbor]);

                    if (neighbor != to){
                        let distanceTo = haversineDist(data.airports[top.title].coords, data.airports[neighbor].coords); 
                        let totalD = distanceTo + top.distance;
                        if (inNodeMap){
                            let ind = nodeMap[neighbor].heapInd;
                            // only update if less than
                            if (totalD < nodeMap[neighbor].distance){
                                pq.updateNode(ind, totalD, top.title);
                            }
                        } else {
                            const str = lexigraphicStr(neighbor, to);
                            let heuristic = 0;
                            if (str in heuristicCoords){
                                heuristic = heuristicCoords[str];
                            } else {
                                heuristic = haversineDist(data['airports'][neighbor].coords, data['airports'][to].coords);
                                heuristicCoords[str] = heuristic;
                            }
                            let gn = new GraphNode(neighbor, top.title, totalD, heuristic,top.connectionsAway + 1);
                            nodeMap[neighbor] = gn;
                            pq.add(gn);
                            // console.log( neighbor + " distance to:  " + (totalD + heuristic) + "     =>" + heuristic) 
                        }
                    } else {
                        let distanceTo = haversineDist(data['airports'][top.title].coords, data['airports'][neighbor].coords);
                        let totalD = distanceTo + top.distance;
                        var path =[]; 
                        
                        path.push(to);
                        let node = top;
                        while (node != null){
                            path.push(node.title);
                            node = nodeMap[node.parent];
                        }
                        
                        
                        // ACTQUEUE.push({'action': 'fly',
                        //     'from': 'LAX', 
                        //     'to': 'YYZ'
                        // });

                        ACTQUEUE.push({'action': 'fly',
                            'from': path[path.length-1],
                            'to': path[path.length-2]
                        });
                        ACTQUEUE.push({'action': 'animatecity', 'city': neighbor, 'type' : 4});
                        PATH = path;
                        TOTALD = totalD;
                        absDistance = haversineDist(data['airports'][PATH[0]].coords, data['airports'][PATH[PATH.length-1]].coords);
                        return {'path' : path, 'distance' : totalD};
                    }

                }

                ACTQUEUE.push({'action': 'delay', 'value' : 400});
                for (var j =0; j < flyToList.length; j++){
                    ACTQUEUE.push({'action': 'fly', 'from': flyToList[j][0].title,
                        'to': flyToList[j][1]});
                }

                iteration++;
            }

            return 'path not found';
        }

    
        // CHAGE TO DIVINCY FORMULA
        // HAVERSINE FORMULA  / great circle distance  
        function haversineDist (coords1, coords2){
            const R = 6371e3; // radius of the earth in metres 
            const φ1 = coords1.latitude * Math.PI/180; // φ, λ in radians
            const φ2 = coords2.latitude * Math.PI/180;
            const Δφ = (coords2.latitude - coords1.latitude) * Math.PI/180;
            const Δλ = (coords2.longitude-coords1.longitude) * Math.PI/180;
            
            const a = Math.sin(Δφ/2) * Math.sin(Δφ/2) +
                    Math.cos(φ1) * Math.cos(φ2) *
                    Math.sin(Δλ/2) * Math.sin(Δλ/2);
            const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
            
            const distance = R * c; // in metres1   
            return distance;
        }

        function lexigraphicStr (str1, str2){
            var sum =0; 
            for (var i =0; i<str1.length; i++){
                sum += str1.charCodeAt(i);
            }
            for (var i =0; i<str2.length; i++){
                sum -= str2.charCodeAt(i);
            }
            return sum >0 ?  str2 +',' + str1 : str1 + ',' +str2;
        }  
        
        // // Add lines
        var lineSeries = chart.series.push(new am4maps.MapArcSeries());
        lineSeries.mapLines.template.line.strokeWidth = 2
        lineSeries.mapLines.template.line.strokeOpacity = 0.5;
        lineSeries.mapLines.template.line.stroke = city.fill;
        lineSeries.mapLines.template.line.nonScalingStroke = true;
        lineSeries.mapLines.template.line.strokeDasharray = "1,1";
        lineSeries.zIndex = 10;
    
        var shadowLineSeries = chart.series.push(new am4maps.MapLineSeries());
        shadowLineSeries.mapLines.template.line.strokeOpacity = 0;
        shadowLineSeries.mapLines.template.line.nonScalingStroke = true;
        shadowLineSeries.mapLines.template.shortestDistance = false;
        shadowLineSeries.zIndex = 5;
    
        // function addLine(from, to) {
        //     var line = lineSeries.mapLines.create(); 
        //     line.imagesToConnect = [from, to];
        //     line.line.controlPointDistance = -0.3;
        //     var shadowLine = shadowLineSeries.mapLines.create();
        //     shadowLine.imagesToConnect = [from, to];
        //     return line;
        // }
        // function addLiner(){
        //     setTimeout(function () {
        //         addLine(newYork, paris);
        //         // shadowLineSeries.mapLines.removeIndex(0);
        //  //         // addCity(data['airports']['LBD'].coords, "LBD", -1);
        //     }, 5000 );    
        // }
        // addLiner();

        async function animateMap() {
            // animateIndex = [];      //     list of start  stop indexes, start inclusive stop exlusive [startind , stopind) 
            
            var delayPeriods = {};
            delayPeriods['loadMap'] = 1000;

            var animateColl = [];
            animateColl.push([]);
            var animateList = [];

            // var prevact = "animatecity";
            // // console.log(ACTQUEUE);

            animateColl[animateColl.length -1].push({'action': 'animatecity', 'city': FROM, 'type' : 3})
            // await delay(delayPeriods.loadMap);
            animateCityCreation(null, null, animateColl[animateColl.length -1], -1);
            var flyList = []
            flyList = [];
            
            for (var i =2; i<ACTQUEUE.length-1; i++){
                // console.log("IN FOR LOOP");
                switch (ACTQUEUE[i].action){
                    case 'animatecity':                        
                        if (ACTQUEUE[i].type == -1) {
                            if (animateColl[animateColl.length-1].length > 10){
                                animateColl.push([]);
                            }
                            animateList.push(ACTQUEUE[i]);
                            // animateColl[animateColl.length-1].push(ACTQUEUE[i]);
                        } else { 
                            // console.log(ACTQUEUE[i])
                            
                            animateCityCreation(ACTQUEUE[i].city, ACTQUEUE[i].type, null, ACTQUEUE[i]['iteration']);
                        }
                        break;
                    case 'fly':
                        // for ( var w =animateColl.length-1; w>=0;  w--){
                        //     await animateCityCreation(null, null, animateColl[w]);
                        // }
                        // return;
                        if (animateList != []){
                            await animateCityCreation(null, -1, animateList)
                            animateList = [];
                        }

                        flyList.push(ACTQUEUE[i]);
                        break;
                    case 'delay':
                        await delay(800)
                        await processFlightList(flyList);
                        flyList = [];
                        requestAnimationFrame(function(){
                            delay(ACTQUEUE[i].value);
                        });
                        break;
                }
            }

            flyList.push ({'action': 'fly', 'from': PATH[PATH.length-1], 'to':PATH[PATH.length-2] });
            await yellowDisBish();

            async function yellowDisBish(){
                for ( var i=0; i< PATH.length; i++ ){
                    let CITY = PATH[i];
                    // console.log(CITY)
                    animateColl[animateColl.length -1].push({'action': 'animatecity', 'city': CITY, 'type' : 4})
                    await animateCityCreation(null, null, animateColl[animateColl.length -1], -1);
                }
            }

                        //             // requestAnimationFrame(function(){
            
                        //     // flyList.push({'action' : 'fly', 'from' : 'KRR', 'to': 'IKA'});
                        //     // flyList.push({'action' : 'fly', 'from' : PATH[PATH.length-1], 'to': PATH[PATH.length-2]});
                        //     for ( var i =0; i<PATH.length-1; i++){
                        //         flyList.push({'action' : 'fly', 'from' : PATH[PATH.length-1 -i], 'to': PATH[PATH.length-2 -i]});
                        //     }
                        // // })
                        // // flyList.push({'action': 'fly', 'from': "YYZ",
                        // //             'to': "KEF"});
                        // // console.log("!!!!!!!!!!!!!!!!!")
                        // await processFlightList(flyList)
                        // flyList = [];
            await delay(5000);
            await go();

        }

        async function animateCityCreation(title, type, list, colorIter){ 
            var i =0;
            if (type == -1){
                for (var i =0; i<list.length; i++){
                    // // console.log(data.airports[list[i]])
                    addCity(list[i].city, null, -1, -69);
                    await delay(10);
                }
            } else {
                
                timeout();
                async function timeout() {
                    
                    requestAnimationFrame(async function () {
                        if (i != DIVISOR-1){
                            if (list != null){
                                for (var j =0; j <list.length; j++){
                                    addCity(list[j].city, i, list[j].type, list);
                                }
                            } else {
                                addCity(title, i, type, colorIter);
                            }
                            timeout();
                        }
                        i++;
                    }, 0); // want this to load right away 
                }            
            }

        }

        function addCity(title, iteration, type, colorIter) {
            const rad = city.radius
            const fill = city.fill;
            const coords = data.airports[title].coords;
            // if ( title === startAirport){
            //     // add first animation 
            //     city.radius= 10.5;
            //     city.fill = "#fff"
            // } else if (iteration != -1){
            
            switch (type ){
                case -1: 
                    city.radius = sizes['start'];
                    city.strokeWidth = city.radius /3;
                    city.fill  = colors[1];
                    break;
                case 1:
                    city.radius = 0 + sizes['start']/DIVISOR * iteration;
                    city.strokeWidth = city.radius /3;
                    city.fill = startingCols[iteration];
                    break;
                case 2: 
                    city.radius = sizes['start'] + (sizes['end'] - sizes['start'])/DIVISOR * iteration;
                    city.strokeWidth = city.radius /3;
                    if (MAXVISITS > 0){
                        city.fill = colors2[Math.round(iteration/(MAXVISITS) + (colorIter - 2) * DIVISOR/(MAXVISITS)) ];
                        // city.fill = colors[iteration];
                    } else {
                        city.fill = colors[iteration];
                    }
                    break;
                case 3:
                    city.radius = 0 + (sizes['end'] - 0)/DIVISOR * iteration;
                    city.strokeWidth = city.radius /3;
                    
                    city.fill = colors[iteration];
                    break;
                case 4: 

                    city.radius = sizes['end'];
                    city.strokeWidth = city.radius /3;
                    
                    city.fill = yellowCols[iteration];
                    break;
                    // city.radius = sizes["end"];
                    // city.strokeWidth = city.radius /3;
                    // city.fill = yellowCols[iteration];
                    // break;
            }
                    // city.radius = 0 + sizes['start']/DIVISOR * iteration;
                    // city.strokeWidth = city.radius /3;
                    // city.fill = colors[iteration];
            // }
            var newcity = cities.mapImages.create();  
            newcity.latitude = coords.latitude;
            newcity.longitude = coords.longitude;
            newcity.tooltipText = title;
    
            city.radius = rad;
            city.fill = fill;
            
            cityObjects[title] = newcity;
            return newcity;
        }
        


        async function processFlightList (flyList){
            // await delay(planeAnimationTime);
            var c  =0 ; 
            for (var i =0; i<flyList.length; i++){
                // requestAnimationFrame(function(){
                // })
                flyiteration = 0;
                c++;
                
                if (c>=15){
                    if (c % 15 == 0){
                        await delay(planeAnimationTime)
                        // fly(flyList[i].from, flyList[i].to, 0);
                        // break;
                    }
                }  else {
                    // requestAnimationFrame(function(){
                        fly(flyList[i].from, flyList[i].to);
                        // })
                    

                }
                flyiteration = 0;
            }
            // await delay(300);
        }

        function delay(ms){
            return new Promise(res => setTimeout(res,ms));
        }
    
        var planeImages = {};
        var flyiteration =0; 
        var to = null;
        var from = null;

        
        // function addLine(to, from){
        //     var lineSeries = chart.series.push(new am4maps.MapArcSeries());
        //     lineSeries.mapLines.template.line.strokeWidth = 2
        //     lineSeries.mapLines.template.line.strokeOpacity = 0.5;
        //     lineSeries.mapLines.template.line.stroke = city.fill;
        //     lineSeries.mapLines.template.line.nonScalingStroke = true;
        //     lineSeries.mapLines.template.line.strokeDasharray = "1,1";
        //     lineSeries.zIndex = 10;
            
        //     // var shadowLineSeries = chart.series.push(new am4maps.MapLineSeries());
        //     // shadowLineSeries.mapLines.template.line.strokeOpacity = 0;
        //     // shadowLineSeries.mapLines.template.line.nonScalingStroke = true;
        //     // shadowLineSeries.mapLines.template.shortestDistance = false;
        //     // shadowLineSeries.zIndex = 5;
    
        //     var line = lineSeries.mapLines.create(); 
        //     line.imagesToConnect = [to, from];
        //     line.line.controlPointDistance = -0.3;
    
        //     // var shadowLine = shadowLineSeries.mapLines.create();
        //     // shadowLine.imagesToConnect = [to, from];

        // }

        // function

        async function fly(flyfrom, flyto, addline){

            to = cityObjects[flyto];
            from = cityObjects[flyfrom];
            
            var t = to;
            to = from;
            from =t;
      
            // // Shrink plane 
            flyiteration ++;
            if (flyiteration >1 ){
            //     plimage.scale = 0;
            //     // const iterTo = 300;
            //     // var i =0; 
            //     // timeout();
            //     // function timeout() {
            //     //     setTimeout(function () {
            //     //         if (i != iterTo-1){
            //     //             // put exponential here 
            //     //             plimage.scale = plimage.scale - 0.08/iterTo *i;
            //     //             timeout();
            //     //         }
            //     //         i++;
            //     //     }, 100);
            //     // }
                return;
            }

            // Add lines and path
            
            var lineSeries = chart.series.push(new am4maps.MapArcSeries());
            lineSeries.mapLines.template.line.strokeWidth = 2
            lineSeries.mapLines.template.line.strokeOpacity = 0.5;
            lineSeries.mapLines.template.line.stroke = city.fill;
            lineSeries.mapLines.template.line.nonScalingStroke = true;
            lineSeries.mapLines.template.line.strokeDasharray = "1,1";
            lineSeries.zIndex = 10;
    
            var shadowLineSeries = chart.series.push(new am4maps.MapLineSeries());
            shadowLineSeries.mapLines.template.line.strokeOpacity = 0;
            shadowLineSeries.mapLines.template.line.nonScalingStroke = true;
            shadowLineSeries.mapLines.template.shortestDistance = false;
            shadowLineSeries.zIndex = 5;

    
            var line = lineSeries.mapLines.create(); 
            line.imagesToConnect = [to, from];
            line.line.controlPointDistance = -0.3;
    
            var shadowLine = shadowLineSeries.mapLines.create();
            shadowLine.imagesToConnect = [to, from];

            if (addline != null){
                return;
            }

            // //  Animate lines being created 
            // const iterTo = 500;
            // var i =0; 
            // var line = null;
            // timeout();
            // function timeout() {
            //     setTimeout(function () {
            //         if (i != iterTo-1){
            //             // put exponential here 
            //             lineSeries.mapLines.template.line.strokeWidth = i*(1.5/iterTo)
            //             line = lineSeries.mapLines.create(); 
            //             line.imagesToConnect = [to, from];
            //             line.line.controlPointDistance = -0.3;
            //             timeout();
            //         }
            //         i++;
            //     }, 200);
            // }
            // var shadowLine = shadowLineSeries.mapLines.create();
            // shadowLine.imagesToConnect = [to, from];
            
            
            // PLANE AND SHADOW PLANE CONFIG ###############
            var plane = lineSeries.mapLines.getIndex(0).lineObjects.create();
            plane.position = 0;
            plane.width = 48;
            plane.height = 48;
            
            plane.adapter.add("scale", function(scale, target) {
                return 0.5 * (1 - (Math.abs(0.5 - target.position)));
            })
    

            var shadowPlane = shadowLineSeries.mapLines.getIndex(0).lineObjects.create();
            shadowPlane.position = 0;
            shadowPlane.width = 48;
            shadowPlane.height = 48;
    
            shadowPlane.adapter.add("scale", function(scale, target) {
                target.opacity = (0.6 - (Math.abs(0.5 - target.position)));
                return 0.5 - 0.3 * (1 - (Math.abs(0.5 - target.position)));
            })
            
    
            var planeImage = plane.createChild(am4core.Sprite);
            planeImage.scale = 0.08;
            planeImage.horizontalCenter = "middle";
            planeImage.verticalCenter = "middle";
            planeImage.path = "m2,106h28l24,30h72l-44,-133h35l80,132h98c21,0 21,34 0,34l-98,0 -80,134h-35l43,-133h-71l-24,30h-28l15,-47";
            planeImage.fill = chart.colors.getIndex(2).brighten(-0.2);
            planeImage.strokeOpacity = 0;

            // plimage = planeImage;
            planeImages[flyfrom + flyto] = planeImage;  

            var shadowPlaneImage = shadowPlane.createChild(am4core.Sprite);
            shadowPlaneImage.scale = 0.05;
            shadowPlaneImage.horizontalCenter = "middle";
            shadowPlaneImage.verticalCenter = "middle";
            shadowPlaneImage.path = "m2,106h28l24,30h72l-44,-133h35l80,132h98c21,0 21,34 0,34l-98,0 -80,134h-35l43,-133h-71l-24,30h-28l15,-47";
            shadowPlaneImage.fill = am4core.color("#red");
            shadowPlaneImage.strokeOpacity = 0;
    
            // const obj = {'plane' : plane,'shadowPlane' : shadowPlane, 'planeImage' : planeImage, 'shadowPlaneImage': shadowPlaneImage};
            plane.mapLine = lineSeries.mapLines.getIndex(0);
            plane.parent = lineSeries;
    
            shadowPlane.mapLine = shadowLineSeries.mapLines.getIndex(0);
            shadowPlane.parent = shadowLineSeries
            shadowPlaneImage.rotation = planeImage.rotation;
    

            var animation = plane.animate({
                from: 0,
                to: 1, 
                property: "position"
            }, planeAnimationTime, am4core.ease.sinInOut);
            animation.events.on("animationended", fly)

            
            var shadowAnimation = shadowPlane.animate({
                from: 0,
                to: 1,
                property: "position"
            }, planeAnimationTime, am4core.ease.sinInOut);
        
            from = null;
            to = null;

            
            requestAnimationFrame(function () {
                setTimeout(() => {
                    animation.kill();
                    plane.hide();
                    shadowPlane.hide();
                    shadowAnimation.kill()  
                },planeAnimationTime);
                // DO SOME SHIT IN HERE animate disappearance
            }, planeAnimationTime);  
            
        }

        function colorAnimation(initObj, targObj, currObj){
            
            var gradientr = (targObj.r-initObj.r)/DIVISOR;
            var gradientg = (targObj.g-initObj.g)/DIVISOR ;
            var gradientb = (targObj.b-initObj.b)/DIVISOR ;   
            
            let objCopy = {'r': currObj.r, 'g' : currObj.g, 'b': currObj.b};
    
            if (gradientr >0){
                if (objCopy.r < targObj.r ) objCopy.r += gradientr;
            } else if (gradientr <0 ){
                if (objCopy.r > targObj.r) objCopy.r += gradientr
            }
            if (gradientg >0){
                if (objCopy.g < targObj.g ) objCopy.g += gradientg;
            } else if (gradientg <0 ){
                if (objCopy.g > targObj.g) objCopy.g += gradientg;
            }
            if (gradientb >0){
                if (objCopy.b <  targObj.b ) objCopy.b += gradientb;
            } else if (gradientb <0 ){
                if (objCopy.b > targObj.b) objCopy.b += gradientb
            }
    
            let data = {};
            var newString = "rgb(" + Math.round(objCopy.r).toString(10) + "," +   
                Math.round(objCopy.g).toString(10) + "," + 
                Math.round(objCopy.b).toString(10) +  ")"; 
    
            data['string'] = newString;
            data['object'] = objCopy;
            return data;
        }

    
        function fillColorsArray(){
            const startCol = {'r' : 64, 'g': 155, 'b' : 208};
            const currCol = {'r' : 64, 'g': 155, 'b' : 208};
            const targCol = {'r' : 144, 'g': 12, 'b' : 63};

            
            const startCol2 = {'r': 51, 'g': 255, 'b': 253};
            const currCol2 = {'r': 51, 'g': 255, 'b': 253};
            const targCol2 = {'r' : 144, 'g': 12, 'b' : 63};
            
            const white = {'r' : 64, 'g': 155, 'b': 208};
            const currWhiteCol = {'r' : 64, 'g': 155, 'b': 208};
            const targStartCol = {'r' : 255, 'g': 255, 'b' : 255};
            
            const endCol = {'r' : 144, 'g': 12, 'b' : 63};
            const curryelCol = {'r' : 144, 'g': 12, 'b' : 63};            
            const yellowTarg = {'r' :255, 'g': 246,  'b': 161};


            // const endCol = {'r' : 177, 'g': 236, 'b' : 213};
            // const curryelCol = {'r' : 177, 'g': 236, 'b' : 213};
            // const yellowTarg = {'r' :255, 'g': 246,  'b': 161};

            
            var col = colorAnimation(startCol, targCol, currCol); 
            var col2 = colorAnimation(startCol2, targCol2, currCol2); 
            var colW = colorAnimation(white, targStartCol, currWhiteCol); 
            var colY = colorAnimation(endCol, yellowTarg, curryelCol);

            for (var i =1; i<DIVISOR ; i++ ){
                col = colorAnimation(startCol, targCol, col.object);
                colors[i] = col.string;
                
                col2 = colorAnimation(startCol2, targCol2, col2.object);
                colors2[i] = col2.string;

                colW = colorAnimation(white, targStartCol, colW.object);
                startingCols[i] = colW.string;

                colY = colorAnimation(endCol, yellowTarg, colY.object);
                yellowCols[i] = colY.string;
            }
        }
        // console.log(data)   

        
        async function go(){            

            
            // Add lines
            var lineSeries = chart.series.push(new am4maps.MapArcSeries());
            lineSeries.mapLines.template.line.strokeWidth = 10  ;
            lineSeries.mapLines.template.line.strokeOpacity = 0.5;
            // lineSeries.mapLines.template.line.stroke = city.fill;
            
            lineSeries.mapLines.template.line.stroke =  {'r' :255, 'g': 204,  'b': 17};
            // lineSeries.mapLines.template.line.stroke.t =
            
            
            lineSeries.mapLines.template.line.nonScalingStroke = true;
            lineSeries.mapLines.template.line.strokeDasharray = "1,1";
            lineSeries.zIndex = 10;
            
            var shadowLineSeries = chart.series.push(new am4maps.MapLineSeries());
            shadowLineSeries.mapLines.template.line.strokeOpacity = 0;
            shadowLineSeries.mapLines.template.line.nonScalingStroke = true;
            shadowLineSeries.mapLines.template.shortestDistance = false;
            shadowLineSeries.zIndex = 5;
            
            function addLine(from, to) {
                var line = lineSeries.mapLines.create();
                line.imagesToConnect = [from, to];
                line.line.controlPointDistance = -0.3;
                
                var shadowLine = shadowLineSeries.mapLines.create();
                shadowLine.imagesToConnect = [from, to];
                
                // console.log("!!!!!!!!!!!")
                return line;
            }
            
            // addLine(paris, toronto);
            // addLine(toronto, la);
            // addLine(la, havana);
            
            for (var i =0; i< PATH.length-1; i++){   
                addLine(cityObjects[PATH[PATH.length-(i+1)]], cityObjects[PATH[PATH.length-(i+2)]]);
            }
            
            // Add plane
            var plane = lineSeries.mapLines.getIndex(0).lineObjects.create();
            plane.position = 0;
            plane.width = 48;
            plane.height = 48;
            
            plane.adapter.add("scale", function(scale, target) {
                return 0.5 * (1 - (Math.abs(0.5 - target.position)));
            })

            var planeImage = plane.createChild(am4core.Sprite);
            planeImage.scale = 0.15;
            planeImage.horizontalCenter = "middle";
            planeImage.verticalCenter = "middle";
            planeImage.path = "m2,106h28l24,30h72l-44,-133h35l80,132h98c21,0 21,34 0,34l-98,0 -80,134h-35l43,-133h-71l-24,30h-28l15,-47";
            planeImage.fill = chart.colors.getIndex(2).brighten(-0.2);
            planeImage.strokeOpacity = 0;

            var shadowPlane = shadowLineSeries.mapLines.getIndex(0).lineObjects.create();
            shadowPlane.position = 0;
            shadowPlane.width = 48;
            shadowPlane.height = 48;

            var shadowPlaneImage = shadowPlane.createChild(am4core.Sprite);
            shadowPlaneImage.scale = 0.06;
            shadowPlaneImage.horizontalCenter = "middle";
            shadowPlaneImage.verticalCenter = "middle";
            shadowPlaneImage.path = "m2,106h28l24,30h72l-44,-133h35l80,132h98c21,0 21,34 0,34l-98,0 -80,134h-35l43,-133h-71l-24,30h-28l15,-47";
            shadowPlaneImage.fill = am4core.color("#000");
            shadowPlaneImage.strokeOpacity = 0;

            shadowPlane.adapter.add("scale", function(scale, target) {
                target.opacity = (0.6 - (Math.abs(0.5 - target.position)));
                return 0.5 - 0.3 * (1 - (Math.abs(0.5 - target.position)));
            })

            // Plane animation
            var currentLine =  0;
            var direction = 1;
            function flyPlane() {

                // Get current line to attach plane to
                plane.mapLine = lineSeries.mapLines.getIndex(currentLine);
                plane.parent = lineSeries;
                shadowPlane.mapLine = shadowLineSeries.mapLines.getIndex(currentLine);
                shadowPlane.parent = shadowLineSeries;
                shadowPlaneImage.rotation = planeImage.rotation;

                // Set up animation
                var from, to;
                var numLines = lineSeries.mapLines.length;
                if (direction == 1) {
                    from = 0
                    to = 1;
                    if (planeImage.rotation != 0) {
                        planeImage.animate({ to: 0, property: "rotation" }, 1000).events.on("animationended", flyPlane);
                        return;
                    }
                }
                else {
                    from = 1;
                    to = 0;
                    if (planeImage.rotation != 180) {
                        planeImage.animate({ to: 180, property: "rotation" }, 1000).events.on("animationended", flyPlane);
                        return;
                    }
                }

                // Start the animation
                var animation = plane.animate({
                    from: from,
                    to: to,
                    property: "position"
                }, 2500, am4core.ease.sinInOut);
                animation.events.on("animationended", flyPlane)
                /*animation.events.on("animationprogress", function(ev) {
                var progress = Math.abs(ev.progress - 0.5);
                //console.log(progress);
                //planeImage.scale += 0.2;
                });*/

                shadowPlane.animate({
                    from: from,
                    to: to,
                    property: "position"
                }, 2500, am4core.ease.sinInOut);

                // Increment line, or reverse the direction
                currentLine += direction;
                if (currentLine < 0) {
                    currentLine = 0;
                    direction = 1;
                }
                else if ((currentLine + 1) > numLines) {
                    currentLine = numLines - 1;
                    direction = -1;
                }

            }
            flyPlane();
        }
    }
}