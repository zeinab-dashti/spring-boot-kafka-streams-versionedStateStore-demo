package space.zeinab.demo.kafka.topology;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.VersionedBytesStoreSupplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;
import space.zeinab.demo.kafka.model.Customer;
import space.zeinab.demo.kafka.model.Order;

import java.time.Duration;

@Component
@RequiredArgsConstructor
public class VersionedTopology {
    VersionedBytesStoreSupplier customerVersionedStoreSupplier =
            Stores.persistentVersionedKeyValueStore("versioned-customers-store", Duration.ofMinutes(30));
    VersionedBytesStoreSupplier ordersVersionedStoreSupplier =
            Stores.persistentVersionedKeyValueStore("versioned-orders-store", Duration.ofHours(30));

    private final ObjectMapper objectMapper;

    @Autowired
    public void process(StreamsBuilder streamsBuilder) {
        KTable<String, Customer> customers = streamsBuilder.table(
                "customers-versioned-topic",
                Consumed.with(Serdes.String(), getCustomerSerde()),
                Materialized.<String, Customer>as(customerVersionedStoreSupplier)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(getCustomerSerde())
        );

        KTable<String, Order> orders = streamsBuilder.table(
                "orders-versioned-topic",
                Consumed.with(Serdes.String(), getOrderSerde()),
                Materialized.<String, Order>as(ordersVersionedStoreSupplier)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(getOrderSerde())
        );

        KTable<String, String> customerOrderJoin = customers.join(
                orders,
                (customer, order) -> "Utilizing versioned store --> Customer: " + customer.customerName() + " placed order worth " + order.orderAmount()
        );

        customerOrderJoin.toStream().print(Printed.<String, String>toSysOut().withLabel("Output stream"));
    }

    private Serde<Customer> getCustomerSerde() {
        return new JsonSerde<>(Customer.class, objectMapper);
    }

    private JsonSerde<Order> getOrderSerde() {
        return new JsonSerde<>(Order.class, objectMapper);
    }
}