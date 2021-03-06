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

import java.util.List;

import org.onebusaway.gtfs.model.Frequency;
import org.onebusaway.gtfs.model.Trip;
import org.onebusaway.gtfs.services.GtfsMutableRelationalDao;
import org.onebusaway.gtfs_transformer.services.EntityTransformStrategy;
import org.onebusaway.gtfs_transformer.services.GtfsTransformStrategy;
import org.onebusaway.gtfs_transformer.services.TransformContext;
import org.onebusaway.gtfs_transformer.updates.UpdateLibrary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoTripsWithBlockIdAndFrequenciesStrategy implements GtfsTransformStrategy {

    private static Logger _log = LoggerFactory.getLogger(org.onebusaway.gtfs_transformer.updates.DeduplicateServiceIdsStrategy.class);

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public void run(TransformContext context, GtfsMutableRelationalDao dao) {

        for (Trip trip : dao.getAllTrips()) {
            List<Frequency> frequencies = dao.getFrequenciesForTrip(trip);
            if (!frequencies.isEmpty() && !isLabelOnly(frequencies)
                    && trip.getBlockId() != null) {
                trip.setBlockId(null);
            }
        }

        UpdateLibrary.clearDaoCache(dao);
    }

    private boolean isLabelOnly(List<Frequency> frequencies) {
        for (Frequency frequency : frequencies) {
            if (frequency.getLabelOnly() == 0) {
                return false;
            }
        }
        return true;
    }
}