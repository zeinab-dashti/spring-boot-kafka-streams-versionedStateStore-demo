package space.zeinab.demo.kafka.topology;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;
import space.zeinab.demo.kafka.model.Customer;
import space.zeinab.demo.kafka.model.Order;

@Component
@RequiredArgsConstructor
public class UnVersionedTopology {
    KeyValueBytesStoreSupplier customerStoreSupplier = Stores.persistentKeyValueStore("customers-store");
    KeyValueBytesStoreSupplier ordersStoreSupplier = Stores.persistentKeyValueStore("orders-store");

    private final ObjectMapper objectMapper;

    @Autowired
    public void process(StreamsBuilder streamsBuilder) {
        KTable<String, Customer> customers = streamsBuilder.table(
                "customers-unversioned-topic",
                Materialized.<String, Customer>as(customerStoreSupplier)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(getCustomerSerde())
        );

        KTable<String, Order> orders = streamsBuilder.table(
                "orders-unversioned-topic",
                Consumed.with(Serdes.String(), getOrderSerde()),
                Materialized.<String, Order>as(ordersStoreSupplier)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(getOrderSerde())
        );

        KTable<String, String> customerOrderJoin = customers.join(
                orders,
                (customer, order) -> "Utilizing unversioned store --> Customer: " + customer.customerName() + " placed order worth " + order.orderAmount()
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