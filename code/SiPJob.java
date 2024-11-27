package SiP; //Safety in Plants

...

public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ObjectMapper mapper = new ObjectMapper();
        /* if you would like to use runtime configuration properties, uncomment the lines below
         * DataStream<String> input = createSourceFromApplicationProperties(env);
         */

        DataStream<String> input = createSourceFromStaticConfig(env);

        /** check if at least PERCENTAGE of frontal images has confidence
         * above MIN_CONFIDENCE(from rekognition) for head cover being detected and worn
         **/
        DataStream<PPEPercentageData> percentages_ppe = input.filter(s -> s.contains("ProtectiveEquipmentModelVersion"))
                .map(e -> {
                        try {
                            return mapper.readValue(e, RekoResult.class);
                        } catch (IOException ioException) {
                            System.err.println("Error deserializing JSON string: " + ioException.getMessage());
                            // Skip the problematic data by returning null, which is filtered out in the next step
                            return null;
                        }
                    })  
                .filter(result -> result != null) // Filter out nulls caused by deserialization failures
                .filter(r -> r.getSourceInfo().camera.type.equals("FRONT"))
                .keyBy(RekoResult::getDeviceGroupingKey)
                .flatMap(new CountWindowPPEAverage());

        /** check if at least PERCENTAGE of up camera images has
         * number of tags equal to number of people
         **/
        DataStream<PeopleCountPercentageData> percentages_people_count = input.filter(s -> s.contains("ProtectiveEquipmentModelVersion"))
                .map(e -> {
                        try {
                            return mapper.readValue(e, RekoResult.class);
                        } catch (IOException ioException) {
                            System.err.println("Error deserializing JSON string: " + ioException.getMessage());
                            // Skip the problematic data by returning null, which is filtered out in the next step
                            return null;
                        }
                    })
                .filter(result -> result != null) // Filter out nulls caused by deserialization failures
                .filter(r -> r.getSourceInfo().camera.type.equals("UP"))
                .keyBy(RekoResult::getDeviceGroupingKey)
                .flatMap(new CountWindowPeopleMatchAverage());

        //percentages_ppe.print();
        //percentages_people_count.print();

         /**
         * join streams
         **/
        DataStream<JoinedPPEData> joinedStreams = percentages_ppe.join(percentages_people_count)
                .where(PPEPercentageData::extractKey)
                .equalTo(PeopleCountPercentageData::extractKey)
                .window(SlidingProcessingTimeWindows.of(Time.milliseconds(1000), Time.milliseconds(100)))
                //.window(TumblingProcessingTimeWindows.of(Time.milliseconds(1500)))
                .apply((JoinFunction<PPEPercentageData, PeopleCountPercentageData, JoinedPPEData>) 
                    (percentage_ppe, percentage_people_count) -> new JoinedPPEData(
                        percentage_ppe.getRequestId(),
                        percentage_ppe.getFactoryId(),
                        percentage_ppe.getGatewayId(),
                        percentage_ppe.getCamId(),
                        percentage_ppe.getCoupleCamera(),
                        percentage_ppe.getActiveMachine(),
                        percentage_ppe.getPPEPercentage(),
                        percentage_people_count.getPeopleCountPercentage()
                    ),
                    TypeInformation.of(new TypeHint<JoinedPPEData>() {})
                );

        //joinedStreams.print();
        /**
         * sliding window produces duplicates => emit only one value for the same request
         */
        DataStream<JoinedPPEData> result = joinedStreams
                .keyBy(JoinedPPEData::joinFilteringKey)
                .process(new DistinctFunction());

        result.print();

        /**
         * apply rules to generate alerts
         */
        DataStream<Tuple2<String,String>> alerts =
                result.filter(//condition to create alert:
                                    //.1 machine active
                                    //.2 ppe condition not satisfied or people and tags number not matching
                                    r ->  r.getActiveMachine() && (r.getPPEPercentage() < PERCENTAGE || r.getPeopleCountPercentage() < PERCENTAGE))
                              .map(
                                        r -> new Tuple2<String,String>(r.getRequestId(), Instant.now().toString())  
                              ).returns(TypeInformation.of(new TypeHint<Tuple2<String, String>>() {}));
                              
        alerts.print();

        alerts.map(t -> {
                try {
                    invokeFunction(awsLambda, functionName, t.f0, t.f1);
                    consecutiveFailures = 0; // Reset the failure counter on success
                } catch (LambdaException e) {
                    consecutiveFailures++;
                    System.err.println("Failed to invoke AWS Lambda: " + e.getMessage());
                    if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
                        System.err.println("Max consecutive Lambda invocation failures reached. Sending alert to monitor.");
                       
                    }
                }
                return t;
            }).returns(TypeInformation.of(new TypeHint<Tuple2<String, String>>() {}));

        env.execute("Flink Streaming Java API For Safety On Plants");

    }
}
