/**
 * Copyright (C) 2023 Leonard Ehrenfried <mail@leonard.io>
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
package org.onebusaway.gtfs.serialization;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;
import org.onebusaway.csv_entities.exceptions.CsvEntityIOException;
import org.onebusaway.gtfs.GtfsTestData;
import org.onebusaway.gtfs.model.Location;
import org.onebusaway.gtfs.model.Stop;
import org.onebusaway.gtfs.model.StopArea;
import org.onebusaway.gtfs.model.StopLocation;

public class FlexReaderTest extends BaseGtfsTest {

  private static final String AGENCY_ID = "1";

  @Test
  public void pierceTransitStopAreas() throws CsvEntityIOException, IOException {
    var dao = processFeed(GtfsTestData.getPierceTransitFlex(), AGENCY_ID, false);

    var areaElements = List.copyOf(dao.getAllStopAreaElements());
    assertEquals(15, areaElements.size());

    var first = areaElements.get(0);
    assertEquals("4210813", first.getAreaId().getId());
    var stop = first.getStopLocation();
    assertEquals("4210806", stop.getId().getId());
    assertEquals("Bridgeport Way & San Francisco Ave SW (Northbound)", stop.getName());
    assertSame(Stop.class, stop.getClass());

    var areaWithLocation = areaElements.stream().filter(a -> a.getId().toString().equals("1_4210800_area_1076")).findFirst().get();

    var location = areaWithLocation.getStopLocation();
    assertSame(Location.class, location.getClass());

    var stopAreas = List.copyOf(dao.getAllStopAreas());
    assertEquals(2, stopAreas.size());

    var area = getArea(stopAreas, "1_4210813");
    assertEquals(12, area.getStops().size());
    var stop2 = area.getStops().stream().min(Comparator.comparing(StopLocation::getName)).get();
    assertEquals("Barnes Blvd & D St SW", stop2.getName());

    var area2 = getArea(stopAreas, "1_4210800");
    assertEquals(3, area2.getStops().size());

    var names = area2.getStops().stream().map(s -> s.getId().toString()).collect(Collectors.toSet());

    assertEquals(Set.of("1_area_1075", "1_area_1074", "1_area_1076"), names);
  }

  private static StopArea getArea(List<StopArea> stopAreas, String id) {
    return stopAreas.stream().filter(a -> a.getId().toString().equals(id)).findAny().get();
  }
}
