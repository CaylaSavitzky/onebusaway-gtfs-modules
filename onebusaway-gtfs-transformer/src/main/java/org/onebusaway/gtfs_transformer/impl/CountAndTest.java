
package org.onebusaway.gtfs_transformer.impl;

import org.onebusaway.cloud.api.ExternalServices;
import org.onebusaway.cloud.api.ExternalServicesBridgeFactory;
import org.onebusaway.gtfs.model.*;
import org.onebusaway.gtfs.services.GtfsMutableRelationalDao;
import org.onebusaway.gtfs_transformer.services.GtfsTransformStrategy;
import org.onebusaway.gtfs_transformer.services.TransformContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CountAndTest implements GtfsTransformStrategy {

    private final Logger _log = LoggerFactory.getLogger(CountAndTest.class);

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public void run(TransformContext context, GtfsMutableRelationalDao dao) {

        int countSt = 0;
        int countCd = 0;

        int countNoSt = 0;
        int countNoCd = 0;
        int curSerTrips = 0;
        int countNoHs = 0;

        AgencyAndId serviceAgencyAndId = new AgencyAndId();
        for (Trip trip : dao.getAllTrips()) {

            if (dao.getStopTimesForTrip(trip).size() == 0) {
                countNoSt++;
            } else {
                countSt++;
            }

            serviceAgencyAndId = trip.getServiceId();
            if (dao.getCalendarDatesForServiceId(serviceAgencyAndId).size() == 0) {
                countNoCd++;
            }
            else {
                countCd++;
            }

            //check for current service
            Date today = removeTime(new Date());
            ServiceCalendar servCal = dao.getCalendarForServiceId(trip.getServiceId());
            if (servCal == null) {
                //check for current service using calendar dates
                for (ServiceCalendarDate calDate : dao.getCalendarDatesForServiceId(trip.getServiceId())) {
                    Date date = calDate.getDate().getAsDate();
                    if (calDate.getExceptionType() == 1 && date.equals(today)) {
                        curSerTrips++;
                        break;
                    }
                }
            }
            else {
                //check for current service using calendar
                Date start = servCal.getStartDate().getAsDate();
                Date end = servCal.getEndDate().getAsDate();
                if (today.equals(start) || today.equals(end) ||
                        (today.after(start) && today.before(end))) {
                    curSerTrips++;
                }
            }

            if (trip.getTripHeadsign() == null) {
                countNoHs++;
                _log.error("Trip {} has no headsign", trip.getId());
            }
        }

        _log.info("Agency: {}, {}", dao.getAllAgencies().iterator().next().getId(), dao.getAllAgencies().iterator().next().getName());
        _log.info("Routes: {}, Trips: {}, Current Service: {}", dao.getAllRoutes().size(), dao.getAllTrips().size(), curSerTrips);
        _log.info("Stops: {}, Stop times {}, Trips w/ st: {}, Trips w/out st: {}", dao.getAllStops().size(), dao.getAllStopTimes().size(), countSt, countNoSt);
        _log.info("This is the Total trips w/out headsign: {}", countNoHs);

        ExternalServices es =  new ExternalServicesBridgeFactory().getExternalServices();
        if (curSerTrips < 1) {
            es.publishMessage(getTopic(), "Agency: "
                    + dao.getAllAgencies().iterator().next().getId()
                    + " "
                    + dao.getAllAgencies().iterator().next().getName()
                    + " has no current service.");
            throw new IllegalStateException(
                    "There is no current service!!");
        }

        if (countNoHs > 0) {
            es.publishMessage(getTopic(), "Agency: "
                    + dao.getAllAgencies().iterator().next().getId()
                    + " "
                    + dao.getAllAgencies().iterator().next().getName()
                    + " has trips w/out headsign: "
                    + countNoHs);
            es.publishMetric(getNamespace(), "No headsigns", null, null, countNoHs);
            _log.error("There are trips with no headsign");
        }
    }

    private Date removeTime(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        date = calendar.getTime();
        return date;
    }

    private Date add3Days(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DATE, 3);
        date = calendar.getTime();
        return date;
    }

    private String getTopic() {
        return System.getProperty("sns.topic");
    }

    private String getNamespace() {
        return System.getProperty("cloudwatch.namespace");
    }
}