/**
 * Copyright (C) 2011 Brian Ferris <bdferris@onebusaway.org>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.onebusaway.gtfs_transformer.updates;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

import org.onebusaway.csv_entities.CSVLibrary;
import org.onebusaway.csv_entities.CSVListener;
import org.onebusaway.csv_entities.schema.annotations.CsvField;
import org.onebusaway.gtfs.model.*;
import org.onebusaway.gtfs.model.calendar.ServiceDate;
import org.onebusaway.gtfs.serialization.mappings.StopTimeFieldMappingFactory;
import org.onebusaway.gtfs.services.GtfsMutableRelationalDao;
import org.onebusaway.gtfs_transformer.services.GtfsTransformStrategy;
import org.onebusaway.gtfs_transformer.services.TransformContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LLRGeneration implements GtfsTransformStrategy {

    private static Logger _log = LoggerFactory.getLogger(LLRGeneration.class);

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }

    //to be fed in as hastus_file
    @CsvField(optional = true)
    private String hastusFiles;

    @CsvField(optional = true)
    private String stopOrderToShapesCsv;

    @CsvField(optional = true)
    private String stopToStopCsv;

    @CsvField(optional = true)
    private String agency;

    @CsvField(optional = true)
    private String gtfsRouteIdInput = "100479";

    @CsvField(optional = true)
    private String noServiceMarker = "Extended";

    @Override
    public void run(TransformContext context, GtfsMutableRelationalDao dao) {
        //load maps & hastus reader
        HashMap<String, Stop> stopToStopMap = readStops(stopToStopCsv,dao,agency);
        HashMap<StopOrderPattern,AgencyAndId> stopOrderShapesMap =
                readStopOrderToShapes(stopOrderToShapesCsv,dao,agency);


        // work hastus
        List<Trip> trips = readHastusFiles(hastusFiles,dao,agency,stopToStopMap);

        // clean stops and blocks & stuff
        fixStopSeqHeadsignShapeBlockSeq(dao,trips,stopOrderShapesMap);

    }

    public void fixStopSeqHeadsignShapeBlockSeq(GtfsMutableRelationalDao dao, List<Trip> trips,
                                                HashMap<StopOrderPattern,AgencyAndId> stopOrderShapesMap){
        for(Trip trip : trips) {
            List<StopTime> stopTimes = new ArrayList<>();
            stopTimes.addAll(dao.getStopTimesForTrip(trip));
            if (stopTimes==null |stopTimes.size()==0){
                _log.error(trip.getId().toString() + " has no stoptimes");
                continue;
            }
            stopTimes.sort(Comparator.comparingInt(StopTime::getArrivalTime));
            for (int i = 0; i < stopTimes.size(); i++) {
                stopTimes.get(i).setStopSequence(i);
                dao.saveOrUpdateEntity(stopTimes.get(i));
            }
            AgencyAndId shapeId = stopOrderShapesMap.get(StopOrderPattern.getPatternForStopTimes(stopTimes));
            if(shapeId==null){
                int a = 3;
            }
            else{
                int a = 5;
            }
            trip.setShapeId(shapeId);
            trip.setTripHeadsign(stopTimes.get(stopTimes.size()-1).getStop().getName());
        }
    }


//
//
//
//     Managing External Files
//
//
//
//



//    Assigning file names

    public void setStopOrderToShapesCsv(String stopOrderToShapesCsv){
        this.stopOrderToShapesCsv = stopOrderToShapesCsv;
    }
    public void setHastusFiles(String hastusFiles){
        this.hastusFiles = hastusFiles;
    }
    public void setStopToStopCsv(String stopToStopCsv){
        this.stopToStopCsv = stopToStopCsv;
    }

    public void setGtfsRouteIdInput(String gtfsRouteIdInput) {
        this.gtfsRouteIdInput = gtfsRouteIdInput;
    }

    public void setNoServiceMarker(String noServiceMarker) {
        this.noServiceMarker = noServiceMarker;
    }
    //    reading methods

    public List<Trip> readHastusFiles(String files,
                                      GtfsMutableRelationalDao dao, String agency, Map<String,Stop> stopMappings){
        List<Trip> trips = new ArrayList<Trip>();
        for (String file: files.split(";")) {
            readHastusFile(file,dao,agency,stopMappings).stream().forEach(x->trips.add(x));
        }
        return trips;
    }

    public List<Trip> readHastusFile(String file,
                                     GtfsMutableRelationalDao dao, String agency, Map<String,Stop> stopMappings){
        HastusListener listener = new HastusListener();
        listener.setNoServiceMarker(noServiceMarker);
        listener.setNumericalStopMappings(stopMappings);
        listener.setGtfsRouteId(gtfsRouteIdInput);
        read(file, listener, dao, agency);
        return listener.getTrips();
    }

    public HashMap<String, Stop> readStops(String file,
                                           GtfsMutableRelationalDao dao, String agency){
        StopsListener listener = new StopsListener();
        read(file,listener,dao,agency);
        return listener.getMap();
    }

    public HashMap<StopOrderPattern,AgencyAndId> readStopOrderToShapes(String file,
                                                                       GtfsMutableRelationalDao dao, String agency){
        StopOrderToShapeListener listener = new StopOrderToShapeListener();
        read(file,listener,dao,agency);
        return listener.getMap();
    }

    public void read(String file, DaoInterfacingListener listener, GtfsMutableRelationalDao dao, String agency){
        if (agency==null){
            _log.info("agency recieved by LLRGeneration is null, deffering to gtfs");
            agency=dao.getAllAgencies().iterator().next().getId();
        }
        listener.setDao(dao);
        listener.setAgency(agency);
        try {
            if (listener != null) {
                InputStream is = new BufferedInputStream(new FileInputStream(file));
                new CSVLibrary().parse(is, listener);
            }
        } catch (Exception e) {
            _log.error("When working with " + listener.getClass().toString() + " caught  " + e.getMessage());
        }
    }


//
//    Listeners
//

//    Hastus Listener

    private static class HastusListener extends DaoInterfacingListener{
        HashMap<AgencyAndId,Trip> agencyAndIdTripHashMap = new HashMap<>();
        Map<String,Stop> numericalStopMappings;
        Map<String,List<Stop>> nameStopMappings = new HashMap<>();
        String gtfsRouteId;
        private String noServiceMarker = "Extended";

        @Override
        public void setDao(GtfsMutableRelationalDao dao){
            this.dao = dao;
            Collection<Stop> stops = dao.getAllStops();
            for(Stop stop : stops){
                List<Stop> stopsForName = nameStopMappings.get(stop.getName());
                if(stopsForName==null){
                    stopsForName= new ArrayList<>();
                    nameStopMappings.put(stop.getName(),stopsForName);
                }
                stopsForName.add(stop);
            }
        }

        public void setNumericalStopMappings(Map<String,Stop> numericalStopMappings){
            this.numericalStopMappings = numericalStopMappings;
        }
        public void setGtfsRouteId(String routeId){
            this.gtfsRouteId = routeId;
        }
        public void setNoServiceMarker(String noServiceMarker){
            this.noServiceMarker = noServiceMarker;
        }

        @Override
        public void handleLine(List<String> list) throws IOException {
            try {
                HastusData data = new HastusData(list);
                if (data.route==null || data.route.equals("") || data.tripInt < 1000)
                    return;
                if (agencyAndIdTripHashMap.get(data.tripId) == null)
                    agencyAndIdTripHashMap.put(data.tripId, createTripBlockServiceId(dao, data));
                createStopTime(dao, data);
            }
            catch (Exception e){
                _log.info("refusing to create a stoptime for "+ list.toArray().toString());
                return;
            }
        }

        List<Trip> getTrips(){
            return agencyAndIdTripHashMap.values().stream().collect(Collectors.toList());
        }


        Trip createTripBlockServiceId(GtfsMutableRelationalDao dao, HastusData data){
            if(dao.getCalendarForServiceId(data.serviceId)==null) {
                createServiceCalendar(dao, data);
            }
            if(dao.getBlockForId(data.blockId)==null)
                createBlockId(dao,data);
            Trip trip = new Trip();
            trip.setId(data.tripId);
            trip.setTripShortName(data.tripShortName);
            trip.setRoute(dao.getRouteForId(data.gtfsRouteAgencyAndId));
            trip.setServiceId(data.serviceId);
            trip.setBlockId(Integer.toString(data.blockId));
            trip.setDirectionId(data.dir);
            dao.saveEntity(trip);
            UpdateLibrary.clearDaoCache(dao);
            return trip;
        }

        void createServiceCalendar(GtfsMutableRelationalDao dao, HastusData data){
            ServiceCalendar calendar = dao.getCalendarForServiceId(data.serviceId);
            if (calendar == null) {
                calendar = new ServiceCalendar();
                calendar.setServiceId(data.serviceId);
                calendar.setStartDate(getStartDate(dao,data));
                calendar.setEndDate(getEndDate(dao,data));
                if(!data.serviceId.getId().contains(noServiceMarker)){
                    if (data.serviceId.getId().contains("Weekday")) {
                        calendar.setMonday(1);
                        calendar.setTuesday(1);
                        calendar.setWednesday(1);
                        calendar.setThursday(1);
                        calendar.setFriday(1);
                    }
                    if (data.serviceId.getId().contains("Saturday")) {
                        calendar.setSaturday(1);
                    }
                    if (data.serviceId.getId().contains("Sunday")) {
                        calendar.setSunday(1);
                    }
                }

                calendar.setServiceId(data.serviceId);
                dao.saveEntity(calendar);
                UpdateLibrary.clearDaoCache(dao);
            }
        }

        ServiceDate getStartDate (GtfsMutableRelationalDao dao, HastusData data){
            return new ServiceDate(2021,04,21);
        }

        ServiceDate getEndDate (GtfsMutableRelationalDao dao, HastusData data){
            return new ServiceDate(2021,11,21);
        }

        void createBlockId(GtfsMutableRelationalDao dao, HastusData data){
            Block block = new Block();
            block.setId(data.blockId);
            block.setBlockSequence(data.blockId);
            block.setBlockRoute(Integer.valueOf(data.route));
            block.setBlockRun(data.runNumber);
            dao.saveEntity(block);
        }

        void createStopTime(GtfsMutableRelationalDao dao, HastusData data) throws IOException {
            StopTime stopTime = new StopTime();
            stopTime.setTrip(dao.getTripForId(data.tripId));
            int time = StopTimeFieldMappingFactory.getStringAsSeconds(data.time+":00");
            stopTime.setArrivalTime(time);
            stopTime.setDepartureTime(time);
            String hastusStop = data.hastusStop;
            if (hastusStop.matches("-?\\d+")){
                if(numericalStopMappings.get(data.hastusStop)==null){
                    _log.info("This stop didn't have a match in stop_to_stop_csv: " + data.hastusStop +
                            " so we're skipping this stoptime " + stopTime.toString());
                    return;}
                stopTime.setStop(numericalStopMappings.get(data.hastusStop));
            } else{
                List<Stop> stopsForName = nameStopMappings.get(hastusStop);
                if(stopsForName==null){
                    _log.info("This stop didn't have a match in stop_to_stop_csv: " + data.hastusStop +
                            " so we're skipping this stoptime " + stopTime.toString());
                    return;}
                stopTime.setStop(stopsForName.get(0));
            }

            dao.saveEntity(stopTime);
        }

        class HastusData{
            String tripShortName = "Local";
            AgencyAndId serviceId;
            String route;
            String tripName;
            int runNumber;
            String time;
            String hastusStop;
            String dir;
            int blockId;
            AgencyAndId tripId;
            AgencyAndId gtfsRouteAgencyAndId;
            int tripInt;

            HastusData(List<String>list) throws IOException{
                if(list.size()!=7) {
                    list = parse(list.get(0));
                }
                if(list.size()<7 | list.get(1)==null | list.get(1).equals("")){
                    return;
                }
                serviceId = new AgencyAndId(agency,"LLR"+list.get(0));
                route = list.get(1).trim();
                route=route.substring(1);
                runNumber = Integer.valueOf(list.get(3).replace("599 -","").trim());
                tripName = list.get(2);
                tripInt = Integer.parseInt(list.get(2));
                time = list.get(4);
                hastusStop = list.get(5);
                if(hastusStop.equals("Tukwila Int�l Blvd Station"))
                    hastusStop="Tukwila Int'l Blvd Station";
                dir = resolveDirection(list.get(6));
                blockId = getBlockId();
                tripId = new AgencyAndId(agency,serviceId.getId()+tripName);
                gtfsRouteAgencyAndId = new AgencyAndId(agency,gtfsRouteId);

            }

            List<String> parse(String s){
                return Arrays.stream(s.split(";")).map(x->x.trim()).collect(Collectors.toList());
            }
            String resolveDirection(String s) throws IOException {
                if(s.equals("North")) return "1";
                if(s.equals("South")) return "0";
                _log.error("I was fed the following Hastus direction and didn't like it: "+ s);
                return null;
            }
            int getBlockId(){
                return serviceId.hashCode()*31+runNumber;
            }
        }
    }

//    Matching Listeners

    private static class StopsListener extends DaoMatchingListener {
        @Override
        public void handleLine(List<String> list) throws IOException {
            Stop stop = handleStopMatch(list.get(1));
            if(stop==null)
                _log.info("unable to match Hastus stop" + list.get(0) +
                        " to non-existant stop "+agency+"_"+list.get(1) +
                        ". When this stop is referenced by stoptime the line" +
                        " will be removed.");
            map.put(list.get(0),stop);
        }
    }

    private static class StopOrderToShapeListener extends DaoMatchingListener {
        @Override
        public void handleLine(List<String> list) throws IOException{

            int i = 0;
            list = list.stream().filter(s->!s.equals("")).collect(Collectors.toList());
            if(list.size()==0)
                return;
            AgencyAndId[] stopIds = list.subList(1,list.size()).stream().map
                    (s -> handleStopMatch(s).getId()).collect(Collectors.toList()).
                            toArray(new AgencyAndId[list.size()-1]);
            StopOrderPattern stopOrderPattern = new StopOrderPattern(stopIds);
            AgencyAndId shapeId = new AgencyAndId("40",list.get(0));
            dao.getShapePointsForShapeId(shapeId);
            map.put(stopOrderPattern,new AgencyAndId(agency,list.get(0)));
        }

    }

//    Abstract Listeners

    abstract static class DaoMatchingListener extends DaoInterfacingListener {
        protected HashMap map = new HashMap<>();
        protected Stop handleStopMatch(String id) {
            Stop stop = dao.getStopForId(new AgencyAndId(agency,id));
            if(stop==null)
                _log.error("could not find stop id "+agency+"_"+id+
                        " when loading in DaoMatchingListener. Passing null. Expect errors.");
            return stop;
        }
        HashMap getMap(){
            return map;
        }
    }

    abstract static class DaoInterfacingListener implements CSVListener {
        GtfsMutableRelationalDao dao;
        String agency;
        void setDao(GtfsMutableRelationalDao dao){
            this.dao = dao;
        }
        void setAgency(String agency){
            this.agency = agency;
        }
    }
}

