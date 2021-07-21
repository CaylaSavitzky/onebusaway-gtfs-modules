/**
 * Copyright (C) 2011 Google, Inc.
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

import java.io.IOException;
import java.util.*;

import org.junit.Test;
import org.onebusaway.gtfs.model.*;
import org.onebusaway.gtfs.serialization.mappings.StopTimeFieldMappingFactory;
import org.onebusaway.gtfs.services.GtfsMutableRelationalDao;
import org.onebusaway.gtfs.services.GtfsRelationalDao;
import org.onebusaway.gtfs_transformer.AbstractTestSupport;
import org.onebusaway.gtfs_transformer.collections.StopOrderPattern;

import static org.junit.Assert.*;

public class LLRGenerationTest extends AbstractTestSupport {

    @Test
    public void test() throws IOException {
        _gtfs.putAgencies(1);
        _gtfs.putStops(1);
        _gtfs.putRoutes(1);
        _gtfs.putTrips(1, "r0", "sid0");
        _gtfs.putStopTimes("t0", "s0");
        addModification("{\"op\":\"LLR_generation\"}");
        GtfsRelationalDao dao = transform();

        Collection<Frequency> frequencies = dao.getAllFrequencies();
        assertEquals(1, frequencies.size());

        Frequency frequency = frequencies.iterator().next();
        assertSame(dao.getTripForId(new AgencyAndId("a0", "t0")),
                frequency.getTrip());
        assertEquals(StopTimeFieldMappingFactory.getStringAsSeconds("08:00:00"),
                frequency.getStartTime());
        assertEquals(StopTimeFieldMappingFactory.getStringAsSeconds("10:00:00"),
                frequency.getEndTime());
        assertEquals(600, frequency.getHeadwaySecs());
    }


    @Test
    public void testReadStops() throws IOException {
        String filePath = this.getClass().getResource(
                "stop_to_stop_mapping.csv").getPath();
        LLRGeneration _strategy = new LLRGeneration();
        mockGtfsSetup();

        GtfsMutableRelationalDao dao = _gtfs.read();

        _strategy.setStopToStopCsv(filePath);
        HashMap<String, Stop> stopMap = _strategy.readStops(filePath,dao,"40");
        assertEquals(3,stopMap.entrySet().size());
        Iterator<HashMap.Entry<String,Stop>> itt = stopMap.entrySet().iterator();
        assertEquals(itt.next().getValue().getId().toString(),"40_99604");
        assertEquals(itt.next().getValue().getId().toString(),"40_99121");
        assertEquals(itt.next().getValue().getId().toString(),"40_99903");
    }


    @Test
    public void testReadStopSeqToShapes() throws IOException {
        String filePath = this.getClass().getResource(
                "stoptimes_to_shape.csv").getPath();
        LLRGeneration _strategy = new LLRGeneration();
        mockGtfsSetup();

        GtfsMutableRelationalDao dao = _gtfs.read();

        _strategy.setStopToStopCsv(filePath);
        HashMap<StopOrderPattern, AgencyAndId> stopTimesToShapeMap =
                _strategy.readStopOrderToShapes(filePath,dao,"40");

        List<StopTime> stopTimes = new ArrayList<>();
        stopTimes.add(stopTimeBuilder("40","99121",2, dao));
        stopTimes.add(stopTimeBuilder("40","99903",1, dao));
        stopTimes.add(stopTimeBuilder("40","99604",3, dao));
        StopOrderPattern matchingPattern1 = StopOrderPattern.getPatternForStopTimes(stopTimes);
        String a = stopTimesToShapeMap.get(matchingPattern1).toString();
        assertEquals("40_10599038",stopTimesToShapeMap.get(matchingPattern1).toString());

        stopTimes.clear();
        stopTimes.add(stopTimeBuilder("40","99121",1, dao));
        stopTimes.add(stopTimeBuilder("40","99903",2, dao));
        stopTimes.add(stopTimeBuilder("40","99604",3, dao));
        StopOrderPattern matchingPattern2 = StopOrderPattern.getPatternForStopTimes(stopTimes);
        assertEquals(null,stopTimesToShapeMap.get(matchingPattern2));
    }

    @Test
    public void testReadHastusA() throws IOException {
        String stopToStopPath = this.getClass().getResource(
                "stop_to_stop_mapping.csv").getPath();
        String hastusPath = this.getClass().getResource(
                "misordered_hastus_sample_A.ssv").getPath();
        LLRGeneration _strategy = new LLRGeneration();
        mockGtfsSetup();
        String agency = "40";
        GtfsMutableRelationalDao dao = _gtfs.read();

        HashMap<String, Stop> stopMap = _strategy.readStops(stopToStopPath,dao,agency);
        List<Trip> trips = _strategy.readHastusFiles(hastusPath,dao,agency,stopMap);
        assertEquals(6,trips.size());
        Collection<StopTime> stopTimes = dao.getAllStopTimes();
        assertEquals(18,stopTimes.size());
        Collection<Block> blocks = dao.getAllBlocks();
        assertEquals(2,blocks.size());
        Block block = blocks.iterator().next();
        List<Trip> tripsForBlock = dao.getTripsForBlockId(new AgencyAndId(agency,Integer.toString(block.getId())));
        assertEquals(3,tripsForBlock.size());
        String s = tripsForBlock.get(1).getId().toString();
        assertEquals(agency+"_LLRWeekdayDecReduced1008",tripsForBlock.get(1).getId().toString());
    }


    @Test
    public void testReadHastusB() throws IOException {
        String stopToStopPath = this.getClass().getResource(
                "stop_to_stop_mapping.csv").getPath();
        String hastusPath = this.getClass().getResource(
                "misordered_hastus_sample_B.ssv").getPath();
        LLRGeneration _strategy = new LLRGeneration();
        mockGtfsSetup();
        String agency = "40";
        GtfsMutableRelationalDao dao = _gtfs.read();

        HashMap<String, Stop> stopMap = _strategy.readStops(stopToStopPath,dao,agency);
        List<Trip> trips = _strategy.readHastusFiles(hastusPath,dao,agency,stopMap);
        assertEquals(9,trips.size());
        Collection<StopTime> stopTimes = dao.getAllStopTimes();
        assertEquals(19,stopTimes.size());
        Collection<Block> blocks = dao.getAllBlocks();
        assertEquals(3,blocks.size());
        Block block = blocks.iterator().next();
        List<Trip> tripsForBlock = dao.getTripsForBlockId(new AgencyAndId(agency,Integer.toString(block.getId())));
        assertEquals(4,tripsForBlock.size());
        String s = tripsForBlock.get(1).getId().toString();
        assertEquals(agency+"_LLRWeekdayDecReduced1010",tripsForBlock.get(1).getId().toString());
        assertEquals(1,dao.getCalendarForServiceId(new AgencyAndId(agency,"LLRWeekdayDecReduced")).getThursday());
        assertEquals(0,dao.getCalendarForServiceId(new AgencyAndId(agency,"LLRWeekdayDecExtended")).getThursday());
    }


    @Test
    public void testFixStopSeqHeadsignShapeBlockSeq() throws IOException {
        String stopToStopPath = this.getClass().getResource(
                "stop_to_stop_mapping.csv").getPath();
        String stopOrderShapesPath = this.getClass().getResource(
                "stoptimes_to_shape.csv").getPath();
        String hastusPath = this.getClass().getResource(
                "misordered_hastus_sample_A.ssv").getPath();
        LLRGeneration _strategy = new LLRGeneration();
        mockGtfsSetup();
        String agency = "40";
        GtfsMutableRelationalDao dao = _gtfs.read();

        HashMap<String, Stop> stopMap = _strategy.readStops(stopToStopPath, dao, agency);
        HashMap<StopOrderPattern,AgencyAndId> stopOrderShapesMap = _strategy.readStopOrderToShapes(stopOrderShapesPath,dao,agency);
        List<Trip> trips = _strategy.readHastusFiles(hastusPath, dao, agency, stopMap);
        _strategy.fixStopSeqHeadsignShapeBlockSeq(dao, trips, stopOrderShapesMap);
    }

    public void mockGtfsSetup(){
        _gtfs.putLines("agency.txt",
                "agency_id,agency_name,agency_url,agency_timezone,agency_lang,agency_phone,agency_fare_url",
                "40,Sound Transit,http://www.soundtransit.org/,America/Los_Angeles,EN,1-888-889-6368,http://www.soundtransit.org/Fares-and-Passes.xml");
        _gtfs.putLines("stops.txt",
                "stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,zone_id,stop_url,location_type,parent_station,stop_timezone",
                "99903,,Seatac/Airport Stn Rail & Intl Blvd S/S 176 St,,47.4450531,-122.296692,80,,0,,America/Los_Angeles",
                "99121,,Beacon Hill Stn Tun & Beacon Av S/S Lander St,,47.5791245,-122.311279,74,,0,,America/Los_Angeles",
                "99604,,UW / Husky Stadium Link Station,,47.649704,-122.303955,81,,0,,America/Los_Angeles");
        _gtfs.putLines("routes.txt",
                "route_id,agency_id,route_short_name,route_long_name,route_desc,route_type,route_url,route_color,route_text_color",
                "100479,40,Link light rail,,Univ. of Washington - SeaTac/Airport - Angle Lake,0,http://www.soundtransit.org/Schedules/ST-Express-Bus/599,,");
        _gtfs.putCalendars(0);
        _gtfs.putLines("shapes.txt",
                "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled",
                "10599038,47.4450531,-122.296692,1,0.0",
                "10599038,47.5791245,-122.311279,1,0.0",
                "10599038,47.649704,-122.303955,1,0.0",
                "10599039,47.649704,-122.303955,1,0.0",
                "10599039,47.5791245,-122.311279,1,0.0",
                "10599039,47.4450531,-122.296692,1,0.0");
        _gtfs.putLines("trips.txt",
                "route_id,service_id,trip_id,trip_headsign,trip_short_name,direction_id,block_id,shape_id,peak_flag,fare_id");
        _gtfs.putLines("stop_times.txt",
                "trip_id,arrival_time,departure_time,stop_id,stop_sequence,stop_headsign,pickup_type,drop_off_type,shape_dist_traveled");
    }

    public StopTime stopTimeBuilder(String agency, String stopId, int stopSeq, GtfsMutableRelationalDao dao){
        StopTime stopTime = new StopTime();
        stopTime.setStopSequence(stopSeq);
        stopTime.setStop(dao.getStopForId(new AgencyAndId(agency,stopId)));
        return stopTime;
    }


}
