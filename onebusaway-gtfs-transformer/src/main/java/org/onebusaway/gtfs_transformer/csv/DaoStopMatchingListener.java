package org.onebusaway.gtfs_transformer.csv;

import org.onebusaway.gtfs.model.AgencyAndId;
import org.onebusaway.gtfs.model.Stop;
import org.onebusaway.gtfs_transformer.csv.DaoInterfacingListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public abstract class DaoStopMatchingListener extends DaoInterfacingListener {
    private static Logger _log = LoggerFactory.getLogger(DaoStopMatchingListener.class);
    protected HashMap map = new HashMap<>();
    protected Stop handleStopMatch(String id) {
        Stop stop = dao.getStopForId(new AgencyAndId(agency,id));
        if(stop==null)
            _log.error("could not find stop id "+agency+"_"+id+
                    " when loading in DaoStopMatchingListener. Passing null");
        return stop;
    }
    public HashMap getMap(){
        return map;
    }
}