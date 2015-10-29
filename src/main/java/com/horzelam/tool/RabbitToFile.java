package com.horzelam.tool;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.IOUtils;

import com.horzelam.tool.factory.ChannelFactory;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class RabbitToFile extends DefaultConsumer {
    private static final long MAX_ROWS_PER_FILE = 10000;

    private static FileOutputStream outputStream;

    private long counter = 0;

    private String queueName;

    private int fileNr = 0;

    private String currentFileName;

    public RabbitToFile(Channel inputChannel, String queueName) throws FileNotFoundException {
        super(inputChannel);
        this.queueName = queueName;
    }

    public static void main(String[] args) throws IOException {
        String host = "127.0.0.1";
        // 
        String user =  "user";//
        String passwd = "passwd"; 
        String queueName = "some.Q.some.tmp";
        Properties rabbitMQConf = new Properties();

        rabbitMQConf.setProperty(ChannelFactory.HOST_KEY, host);
        rabbitMQConf.setProperty(ChannelFactory.USER_KEY, user);
        rabbitMQConf.setProperty(ChannelFactory.PASSWD_KEY, passwd);

        Channel inChannel = ChannelFactory.createChannel(rabbitMQConf);
        // Channel outChannel =
        // QueueFromConfigFactory.getInstance(rabbitMQConf);

        RabbitToFile handler = new RabbitToFile(inChannel, queueName);

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

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
            throws IOException {
        // this is just tool - all data can be processed sequentially
        synchronized (queueName) {
            if (outputStream == null) {
                fileNr++;
                currentFileName = queueName + ".DUMP." + fileNr + ".txt";
                outputStream = new FileOutputStream(currentFileName);
                System.out.println("Writing data to new file: " + currentFileName);
            }

            // JsonNode payload = MAPPER.readTree(body);
            IOUtils.write(body, outputStream);
            IOUtils.write("\n", outputStream);

            // getChannel().basicNack(envelope.getDeliveryTag(), false, true);
            getChannel().basicAck(envelope.getDeliveryTag(), false);

            if (++counter == MAX_ROWS_PER_FILE) {
                System.out.println("File " + currentFileName + " finished.");
                outputStream.close();
                outputStream = null;
                counter = 0;
            }
        }
        // outChannel.basicPublish(outputExchangeName, routingKey,
        // PUBLISH_PROPS, byteMsg);
    }

}
