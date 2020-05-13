package be.vdab.vdp.InosEventProcessing


import groovy.util.logging.Slf4j
//import io.confluent.ksql.function.udf.Udf
//import io.confluent.ksql.function.udf.UdfDescription
//import io.confluent.ksql.function.udf.UdfParameter

import java.time.*
//
//@Slf4j
//@UdfDescription(
//        name = "forecastGesprekken_Period",
//        description = "Calculate the forecast based on the incoming event.")
//class ForecastGesprekken_Period {
//
//
//
//    @Udf(description = "calculate when calls will happen based on the inos event")
//    public Map<String,Map<String,Long>> forecastGesprekken_Period(
//            @UdfParameter(value = "event")  String event,
//            @UdfParameter(value = "eventTime") long eventTime)
//    {
//        log.debug ("forecastGesprekken called with ${event} and ${eventTime}")
//        return forecastGesprekken_Period(event,eventTime,0l)
//    }
//
//    @Udf(description = "calculate when calls will happen based on the inos event. This by also taking a pauze period into account")
//    public Map<String,Map<String,Long>> forecastGesprekken_Period(
//            @UdfParameter(value = "event")  String event,
//            @UdfParameter(value = "eventTime") long eventTime,
//            @UdfParameter(value = "timeSpendinPauzeInPreviousStep") long timeInPauze){
//
//
//        LocalDateTime eventTimeCasted = Instant.ofEpochMilli(eventTime).toDate().toLocalDateTime()
//        def forecaster = AbstractForecastPredictor.GetInstance(event, eventTimeCasted)
//        return forecaster.datesToMap()
//    }
//
//}
//
//@Slf4j
//@UdfDescription(
//        name = "forecastGesprekken_Weeks",
//        description = "Calculate the forecast based on the incoming event.")
//class ForecastGesprekken_Weeks {
//
////    final SqlStruct outputSchema
////    final SqlStruct beingEndSchema
//    @Udf(description = "calculate when calls will happen based on the inos event")
//    public  Map<String,List<Long>> forecastGesprekken_Weeks(
//            @UdfParameter(value = "event")  String event,
//            @UdfParameter(value = "eventTime") long eventTime)
//    {
//        log.debug ("forecastGesprekken called with ${event} and ${eventTime}")
//        return forecastGesprekken_Weeks(event,eventTime,0l)
//    }
//
//    @Udf(description = "calculate when calls will happen based on the inos event. This by also taking a pauze period into account")
//    public Map<String,List<Long>> forecastGesprekken_Weeks(
//            @UdfParameter(value = "event")  String event,
//            @UdfParameter(value = "eventTime") long eventTime,
//            @UdfParameter(value = "timeSpendinPauzeInPreviousStep") long timeInPauze){
//
//
//        log.debug ("forecastGesprekken called with ${event} and ${eventTime} and pauze ${timeInPauze}")
//
//        LocalDateTime eventTimeCasted = Instant.ofEpochMilli(eventTime).toDate().toLocalDateTime()
//        def forecaster = AbstractForecastPredictor.GetInstance(event, eventTimeCasted)
//        def map = forecaster.weeksToMap()
//
//        return map
//    }
//
//}


abstract class AbstractForecastPredictor {

    static AbstractForecastPredictor  GetInstance(String event, LocalDateTime eventTime){
            switch (event.toUpperCase()) {
                case "INSCHATTINGOPGESTART":
                    return new InschattingOpgestart(eventTime)
                case "INSCHATTINGBEEINDIGD":
                    return new InschattingBeeindigd(eventTime)
                case "INSCHATTINGSGESPREKALSOPDRACHTGEPLAND":
                    return new InschattingsGesprekAlsOpdrachtGepland(eventTime)
                case "INSCHATTINGSGESPREKALSBELLIJSTGEPLAND":
                    return new InschattingsGesprekAlsBellijstGepland(eventTime)
                default:
                    return new InosEventDoesNotAffectPrediction(eventTime)
            }

    }

    public  LocalDateTime eventTime
    ForecastsInfo forecastsInfo

    public boolean EventAffectsPredictions =true

    AbstractForecastPredictor(LocalDateTime eventTime){
        this.eventTime=eventTime
        CalculateIG()
        CalculateOG1()
        CalculateOG2()
    }

    static LocalDateTime GetStartDate(LocalDateTime date){
        return date.toLocalDate().atStartOfDay()
    }

    static LocalTime endOfDayTime =new LocalTime(23,59,59,999999999)

    static LocalDateTime GetEndDate(LocalDateTime date){
        return date.clearTime().plusNanos(endOfDayTime.toNanoOfDay())
    }
    static Tuple2<LocalDateTime,LocalDateTime> GetStartAndEnd(LocalDateTime startDate,int period ){
        new Tuple2<LocalDateTime ,LocalDateTime>(GetStartDate(startDate),GetEndDate(startDate.plusDays(period)))
    }
    static convertDates = { Tuple2<LocalDateTime,LocalDateTime>  input ->  [BeginPeriod:input.first.toInstant(ZoneOffset.UTC).toEpochMilli(), EndPeriod:input.second.toInstant(ZoneOffset.UTC).toEpochMilli()]}

    static List<LocalDateTime>  getOverlappingWeeks(Tuple2<LocalDateTime,LocalDateTime> startEndDates){
        def startDate = startEndDates.getV1().toLocalDate()
        def endPeriod = startEndDates.getV2().toLocalDate()
        def firstMonday = startDate.plusDays(1 - DayOfWeek.from(startDate).value)
        List<LocalDateTime> firstDays=[]
        def nextMonday=firstMonday
        while(nextMonday<endPeriod){
            firstDays <<  nextMonday.atStartOfDay()
            nextMonday=nextMonday.plusWeeks(1)
        }
        return firstDays
    }


    def abstract void  CalculateIG()
    def abstract void  CalculateOG1()
    def abstract void  CalculateOG2()

    def Map<String,Map<String,LocalDateTime >> datesToMap(){
//        if (this.EventAffectsPredictions )
//            return [
//            IG:this.IG==null? null :AbstractForecastPredictor.convertDates(this.IG),
//            OG1:this.OG1==null?null:AbstractForecastPredictor.convertDates(this.OG1),
//            OG2:this.OG2==null?null:AbstractForecastPredictor.convertDates(this.OG2)
//            ]
//        else
//            return null
    }
    def Map<String,List<Long>>  weeksToMap(){
//        if (this.EventAffectsPredictions )
//
//        return [
//                IG:this.IG==null?null:AbstractForecastPredictor.getOverlappingWeeks(this.IG).collect(i ->i.toInstant(ZoneOffset.UTC).toEpochMilli()),
//                OG1:this.OG1==null?null:AbstractForecastPredictor.getOverlappingWeeks(this.OG1).collect(i ->i.toInstant(ZoneOffset.UTC).toEpochMilli()),
//                OG2:this.OG2==null?null:AbstractForecastPredictor.getOverlappingWeeks(this.OG2).collect(i ->i.toInstant(ZoneOffset.UTC).toEpochMilli())
//        ]
//        else
//            return null;
    }
}


class InosEventDoesNotAffectPrediction extends AbstractForecastPredictor{
    InosEventDoesNotAffectPrediction(LocalDateTime eventTime) {
        super(eventTime)
        EventAffectsPredictions=false
    }

    @Override
    void CalculateIG() {

    }

    @Override
    void CalculateOG1() {

    }

    @Override
    void CalculateOG2() {

    }
}

class InschattingOpgestart extends AbstractForecastPredictor{

    InschattingOpgestart(LocalDateTime eventTime) {
        super(eventTime)
    }

    @Override
    void CalculateIG() {
        forecastsInfo.IG= GetStartAndEnd(eventTime.plusDays(35),14)
    }

    @Override
    void CalculateOG1() {
        forecastsInfo.OG1 = GetStartAndEnd(IG.getV1().plusDays(85),14)
    }

    @Override
    void CalculateOG2() {
        forecastsInfo.OG2 = GetStartAndEnd(OG1.getV1().plusDays(85),14)
    }
}


class InschattingsGesprekAlsOpdrachtGepland extends AbstractForecastPredictor{

    InschattingsGesprekAlsOpdrachtGepland(LocalDateTime eventTime) {
        super(eventTime)
    }

    @Override
    void CalculateIG() {
        forecastsInfo.IG=GetStartAndEnd(eventTime.plusDays(1),14)
    }

    @Override
    void CalculateOG1() {
        forecastsInfo.OG1=GetStartAndEnd(IG.getV1().plusDays(85),14)
    }

    @Override
    void CalculateOG2() {
        forecastsInfo.OG2=GetStartAndEnd(OG1.getV1().plusDays(85),14)
    }
}

class InschattingsGesprekAlsBellijstGepland extends AbstractForecastPredictor{

    InschattingsGesprekAlsBellijstGepland(LocalDateTime eventTime) {
        super(eventTime)
    }

    @Override
    void CalculateIG() {
        forecastsInfo.IG=GetStartAndEnd(eventTime.plusDays(0),14)
    }

    @Override
    void CalculateOG1() {
        forecastsInfo.OG1=GetStartAndEnd(IG.getV1().plusDays(85),14)
    }

    @Override
    void CalculateOG2() {
        forecastsInfo.OG2=GetStartAndEnd(OG1.getV1().plusDays(85),14)
    }
}

class InschattingBeeindigd extends AbstractForecastPredictor{

    InschattingBeeindigd(LocalDateTime eventTime) {
        super(eventTime)
    }

    @Override
    void CalculateIG() {

    }

    @Override
    void CalculateOG1() {

    }

    @Override
    void CalculateOG2() {

    }
}
