package be.vdab.vdp.InosEventProcessing

import com.fasterxml.jackson.annotation.JsonIgnore

import java.time.LocalDateTime
import java.time.ZoneOffset

class InosStateEvent {

    int IKL
    String eventIdentifier;
    String eventName;
    Long startDate;
    Long endDate
    String payload


    static DefaultEndDate = LocalDateTime.of(9999,12,31,23,59,59,999999).toInstant(ZoneOffset.UTC).toEpochMilli()

    InosStateEvent(int ikl, String eventIdentifier, String eventName, long eventTime, String payload){
        this.IKL=ikl
        this.eventIdentifier=eventIdentifier
        this.eventName=eventName
        this.startDate=eventTime
        this.payload=payload
    }

    InosStateEvent(int ikl, String eventIdentifier, String eventName, long startDate, long endDate, String payload){
        this.IKL=ikl
        this.eventIdentifier=eventIdentifier
        this.eventName=eventName
        this.startDate=startDate
        this.endDate=endDate
        this.payload=payload
    }

    public setEndDate(Long eventStart){
        this.endDate = eventStart-1
    }

    public setEndDate(){
        this.endDate =DefaultEndDate
    }


}