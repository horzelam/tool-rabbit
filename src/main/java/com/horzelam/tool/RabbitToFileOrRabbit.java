package com.horzelam.tool;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.horzelam.tool.factory.ChannelFactory;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class RabbitToFileOrRabbit extends DefaultConsumer {

    private static final ObjectMapper MAPPER = RabbitToFileOrRabbit.createObjectMapper();

    private static final long MAX_ROWS_PER_FILE = 100;

    private static FileOutputStream outputStream;

    private long written = 0;

    private String queueName;

    private int fileNr = 1;

    private String currentFileName;

    private Channel outChannel;

    private String outputExchName;

    private Set<String> ignoredUUIDs;

    private String currentDate;

    public RabbitToFileOrRabbit(Channel inputChannel, String queueName, Channel outChannel, String outQueueName)
            throws FileNotFoundException {
        super(inputChannel);
        this.queueName = queueName;
        this.outChannel = outChannel;
        this.outputExchName = outQueueName;
        ignoredUUIDs = Sets.newHashSet("200104", "200098", "200104", "200110", "200116", "200119");
        this.currentDate = new String(DateTime.now().toString().substring(0, 18));
    }

    private static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JodaModule());

        SimpleModule simpleModule = new SimpleModule();
        simpleModule.addSerializer(DateTime.class, new DateTimeSerializer());
        mapper.registerModule(simpleModule);

        return mapper;

    }

    public static void main(String[] args) throws IOException {
        String queueName = "some.Q.input.dead";
        String outExchangeName = "some.E.input.tmp";

        Properties rabbitMQConf = new Properties();

        rabbitMQConf.setProperty(ChannelFactory.HOST_KEY, "127.0.0.1");
        rabbitMQConf.setProperty(ChannelFactory.USER_KEY, "username");
        rabbitMQConf.setProperty(ChannelFactory.PASSWD_KEY, "paswd123");

        Channel inChannel = ChannelFactory.createChannel(rabbitMQConf);
        Channel outChannel = ChannelFactory.createChannel(rabbitMQConf);
        // Channel outChannel =
        // QueueFromConfigFactory.getInstance(rabbitMQConf);

        RabbitToFileOrRabbit handler = new RabbitToFileOrRabbit(inChannel, queueName, outChannel, outExchangeName);

        String consumerTag = inChannel.basicConsume(queueName, handler);
        try {
            Thread.sleep(50000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }

    }

    // /49,106
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
            throws IOException {
        // this is just tool - all data can be processed sequentially
        synchronized (queueName) {

            String routingKey = envelope.getRoutingKey();

            String inputMsgBody = new String(body, Charsets.UTF_8);

            // Data msg = extractor.extract(inputMsgBody);
            // boolean shouldBeIgnored = ...
            // boolean shouldBeRepublished = ....
            //
            // if (shouldBeIgnored) {
            // ...
            // } else if (shouldBeRepublished) {
            //
            // outChannel.txSelect();
            // outChannel.basicPublish(outputExchName, routingKey, new
            // BasicProperties.Builder().build(), body);
            // outChannel.txCommit();
            //
            // } else {
            // if (outputStream == null) {
            // fileNr++;
            // currentFileName = currentDate + "." + queueName + ".DUMP." +
            // fileNr + ".txt";
            // outputStream = new FileOutputStream(currentFileName);
            // System.out.println("Writing data to new file: " +
            // currentFileName);
            // }
            //
            // // we don't know what to do with the rest - so write it to file
            // written++;
            // // JsonNode payload = MAPPER.readTree(body);
            // IOUtils.write(body, outputStream);
            // IOUtils.write("\n", outputStream);
            //
            // }

            // getChannel().basicNack(envelope.getDeliveryTag(), false, true);
            getChannel().basicAck(envelope.getDeliveryTag(), false);

            // System.exit(-1);
            if (written == MAX_ROWS_PER_FILE) {
                System.out.println("File " + currentFileName + " finished.");
                outputStream.close();
                outputStream = null;
                written = 0;
                System.exit(-1);
            }

        }
        // outChannel.basicPublish(outputExchangeName, routingKey,
        // PUBLISH_PROPS, byteMsg);
    }

}
