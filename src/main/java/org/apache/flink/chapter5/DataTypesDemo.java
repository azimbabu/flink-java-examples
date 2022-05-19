package org.apache.flink.chapter5;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataTypesDemo {

  public static void main(String[] args) throws Exception {
    // set up the streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    // configure watermark interval
    env.getConfig().setAutoWatermarkInterval(1000L);

    // primitives(env);

    // tuples(env);

    // pojo(env);

    createTypeInformation();

    DataStream<Tuple2<String, Integer>> tuples =
        env.fromElements(Tuple2.of("Adama", 17), Tuple2.of("Sarah", 23));

    tuples
        .map(t -> new Person(t.f0, t.f1))
        // provide TypeInformation for the map lambda function's return type
        .returns(Types.POJO(Person.class));

    // execute the application
    // env.execute("DataTypes Example");
  }

  private static void createTypeInformation() {
    // TypeInformation for primitive types
    TypeInformation<Integer> intType = Types.INT;
    System.out.println(intType);

    // TypeInformation for Java Tuples
    TypeInformation<Tuple2<Long, String>> tupleType = Types.TUPLE(Types.LONG, Types.STRING);
    System.out.println(tupleType);

    // TypeInformation for POJOs
    TypeInformation<Person> personType = Types.POJO(Person.class);
    System.out.println(personType);
  }

  private static void pojo(StreamExecutionEnvironment env) {
    DataStream<Person> persons = env.fromElements(new Person("Alex", 42), new Person("Wendy", 23));
    persons.print();
  }

  private static void tuples(StreamExecutionEnvironment env) {
    // DataStream of Tuple2<String, Integer> for Person(name, age)
    DataStream<Tuple2<String, Integer>> persons =
        env.fromElements(Tuple2.of("Adama", 17), Tuple2.of("Sarah", 23));

    System.out.println("Persons:");
    persons.print();

    // filter for persons of age > 18
    SingleOutputStreamOperator<Tuple2<String, Integer>> filtered = persons.filter(p -> p.f1 > 18);
    System.out.println("Persons over 18:");
    filtered.print();

    Tuple2<String, Integer> personTuple = Tuple2.of("Alex", 42);
    Integer age = personTuple.getField(1); // age = 42
    System.out.println("Person: " + personTuple + ", age: " + age);

    personTuple.f1 = 43;
    System.out.println("Person: " + personTuple);
    personTuple.setField(44, 1);
    System.out.println("Person: " + personTuple);
  }

  private static void primitives(StreamExecutionEnvironment env) {
    DataStreamSource<Long> numbers = env.fromElements(1L, 2L, 3L, 4L, 5L);
    SingleOutputStreamOperator<Long> incrementedNumbers = numbers.map(n -> n + 1);
    incrementedNumbers.print();
  }

  public static class Person {
    // both fields are public
    public String name;
    public int age;

    // default constructor is present
    public Person() {}

    public Person(String name, int age) {
      this.name = name;
      this.age = age;
    }
  }
}
