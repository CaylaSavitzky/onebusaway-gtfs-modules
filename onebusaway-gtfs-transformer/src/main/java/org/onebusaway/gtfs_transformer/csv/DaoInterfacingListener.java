package org.onebusaway.gtfs_transformer.csv;

import org.onebusaway.csv_entities.CSVListener;
import org.onebusaway.gtfs.services.GtfsMutableRelationalDao;

public abstract class DaoInterfacingListener implements CSVListener {
    public GtfsMutableRelationalDao dao;
    public String agency;
    public void setDao(GtfsMutableRelationalDao dao){
        this.dao = dao;
    }
    public void setAgency(String agency){
        this.agency = agency;
    }
}