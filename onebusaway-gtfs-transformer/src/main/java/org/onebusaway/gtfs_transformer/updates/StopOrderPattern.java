package org.onebusaway.gtfs_transformer.updates;

import org.onebusaway.gtfs.model.AgencyAndId;
import org.onebusaway.gtfs.model.StopTime;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class StopOrderPattern {
    private final AgencyAndId[] _stopIds;

    public StopOrderPattern(AgencyAndId[] stopIds) {
        _stopIds = stopIds;
    }

    @Override
    public int hashCode() {
        int hashCode = Arrays.stream(_stopIds).
                map(x->x.toString().hashCode()).
                reduce(1,(a,b)-> 31*a+b);
        int altHashCode = Arrays.hashCode(_stopIds);

        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        StopOrderPattern other = (StopOrderPattern) obj;
        if (!Arrays.equals(_stopIds, other._stopIds))
            return false;
        return true;
    }

    public static StopOrderPattern getPatternForStopTimes(List<StopTime> stopTimes) {
        int n = stopTimes.size();
        AgencyAndId[] stopIds = stopTimes.stream().sorted((a,b)->a.getStopSequence()-b.getStopSequence()).
                map(x->x.getStop().getId()).collect(Collectors.toList()).
                toArray(new AgencyAndId[stopTimes.size()]);
        return new StopOrderPattern(stopIds);
    }
}
