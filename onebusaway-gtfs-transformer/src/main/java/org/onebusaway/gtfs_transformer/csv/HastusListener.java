package org.onebusaway.gtfs_transformer.csv;

import org.onebusaway.gtfs.model.*;
import org.onebusaway.gtfs.model.calendar.ServiceDate;
import org.onebusaway.gtfs.serialization.mappings.StopTimeFieldMappingFactory;
import org.onebusaway.gtfs.services.GtfsMutableRelationalDao;
import org.onebusaway.gtfs_transformer.updates.UpdateLibrary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class HastusListener extends DaoInterfacingListener {
    private static Logger _log = LoggerFactory.getLogger(HastusListener.class);
    HashMap<AgencyAndId, Trip> agencyAndIdTripHashMap = new HashMap<>();
    Map<String, Stop> numericalStopMappings;
    Map<String, List<Stop>> nameStopMappings = new HashMap<>();
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

    public List<Trip> getTrips(){
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
