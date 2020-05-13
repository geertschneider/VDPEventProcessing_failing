package be.vdab.vdp.InosEventProcessing

import groovy.json.JsonSlurper
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.ValueTransformer
import org.apache.kafka.streams.kstream.ValueTransformerSupplier
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.state.Stores

class InosEventProcessor {



    def Setup(StreamsBuilder builder){

        StoreBuilder storeBuilder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("IKL"),Serdes.Integer(),Serdes.String());
        builder.addStateStore(storeBuilder);

        ValueTransformerSupplier <String,Iterable<String>> inosTransformer=createInosStateTransformer()

        KStream<String,String> rawEventStream = builder.stream("STR_INOSEVENTS_RAW", Consumed.with(Topology.AutoOffsetReset.EARLIEST))

        //create the "slowly changing dimension" - start and end date events
        def stateStream=rawEventStream.flatTransformValues(inosTransformer,"IKL")
        stateStream.to("OUT_INOS_STATECHANGES")


    }

    ValueTransformerSupplier<String, Iterable<String>> createInosStateTransformer(){
        new ValueTransformerSupplier<String, Iterable<String>>() {

            @Override
            ValueTransformer<String, Iterable<String>> get() {
                return new ValueTransformer<String, Iterable<String>>() {
                    private KeyValueStore<Integer, String> store
                    def jsonGenerator = new groovy.json.JsonGenerator.Options()
                            .excludeFieldsByName("defaultEndDate")
                            .build()


                    @Override
                    void init(ProcessorContext context) {
                        store = (KeyValueStore<Integer, String>) context.getStateStore("IKL")

                    }

                    @Override
                    Iterable<String> transform(String value) {

                        JsonSlurper slurper = new JsonSlurper()
                        def fromJSON = slurper.parseText(value)
                        def transFomredValues = []
                        int IKL = fromJSON.IKL

                        try{


                            def currentInosEvent = new InosStateEvent(IKL, fromJSON.EVENTIDENTIFIER, fromJSON.EVENTNAME, fromJSON.EVENTTIME, fromJSON.PAYLOAD)
                            currentInosEvent.setEndDate()
                            transFomredValues << jsonGenerator.toJson(currentInosEvent)
                            def prevEventText = store.get(IKL)
                            if (prevEventText != null) {
                                def prevJson = slurper.parseText(prevEventText)
                                def prevInosEvent = new InosStateEvent(IKL, prevJson.eventIdentifier, prevJson.eventName, prevJson.startDate, prevJson.endDate, prevJson.payload)
                                prevInosEvent.setEndDate(currentInosEvent.startDate)
                                transFomredValues << jsonGenerator.toJson(prevInosEvent)
                            }

                            store.put(IKL, transFomredValues[0])
                        }
                        catch(Exception ex){
                            println("Failed to process IKL : ${IKL}")
                        }
                        return transFomredValues

                    }
                    @Override
                    void close() {

                    }
                }
            }

        }
    }
}

