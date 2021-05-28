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
import org.onebusaway.csv_entities.schema.annotations.CsvField;
import org.onebusaway.gtfs.model.*;
import org.onebusaway.gtfs.services.GtfsMutableRelationalDao;
import org.onebusaway.gtfs_transformer.collections.StopOrderPattern;
import org.onebusaway.gtfs_transformer.csv.DaoInterfacingListener;
import org.onebusaway.gtfs_transformer.csv.DaoStopMatchingListener;
import org.onebusaway.gtfs_transformer.csv.HastusListener;
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
        Set<StopOrderPattern> unmatchedStopOrders = new HashSet<>();
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
                unmatchedStopOrders.add(StopOrderPattern.getPatternForStopTimes(stopTimes));
            }
            trip.setShapeId(shapeId);
            trip.setTripHeadsign(stopTimes.get(stopTimes.size()-1).getStop().getName().replace(" Station",""));
        }
        _log.info("logging unmatched stop patterns");
        unmatchedStopOrders.stream().forEach((idList) -> {
            _log.error(Arrays.stream(idList.getStopIds()).map(id->id.toString()).
                    reduce("",(a,b)->{return a+" " +b;}));
        });
        _log.info("unmatched stop patterns logged.");
    }


//
//     Managing External Files
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
                                      GtfsMutableRelationalDao dao, String agency, Map<String,Stop> stopMappings) {
        List<Trip> trips = new ArrayList<Trip>();
        for (String fileInfoWhole: files.split(";")) {
            String[] fileInfo = fileInfoWhole.split(",");
            HastusListener listener = new HastusListener();
            listener.setNoServiceMarker(noServiceMarker);
            listener.setNumericalStopMappings(stopMappings);
            listener.setGtfsRouteId(gtfsRouteIdInput);
            String fileAddress = "";
            String fileId = fileAddress.split("/")[(fileAddress.split("/")).length-1];
            if(fileInfo.length==3) {
                listener.setStartDate(fileInfo[0]);
                listener.setEndDate(fileInfo[1]);
                fileAddress = fileInfo[2];
                fileId = fileInfo[0]+"-"+fileInfo[1];
            }else if(fileInfo.length==1){
                fileAddress = fileInfo[0];

            }else {
                _log.error("Expected Hastus filename format is '[yyyy/mm/dd],[yyyy/mm/dd],[fileName]' or [filename]." +
                        fileInfoWhole + "does not meet this expectation");
            }
            listener.setFileId(fileId);
            read(fileAddress, listener, dao, agency);
            listener.getTrips().stream().forEach(x->trips.add(x));
        }
        return trips;
    }

    public HashMap<String, Stop> readStops(String file,
                                           GtfsMutableRelationalDao dao, String agency){
        StopsListenerStop listener = new StopsListenerStop();
        read(file,listener,dao,agency);
        return listener.getMap();
    }

    public HashMap<StopOrderPattern,AgencyAndId> readStopOrderToShapes(String file,
                                                                       GtfsMutableRelationalDao dao, String agency){
        StopOrderToShapeListenerStop listener = new StopOrderToShapeListenerStop();
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

//  Support Listeners

    private static class StopsListenerStop extends DaoStopMatchingListener {
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

    private static class StopOrderToShapeListenerStop extends DaoStopMatchingListener {
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
}

