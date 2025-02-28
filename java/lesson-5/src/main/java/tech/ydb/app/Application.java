package tech.ydb.app;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.TopicReadSettings;
import tech.ydb.topic.settings.WriterSettings;
import tech.ydb.topic.write.Message;

/**
 * @author Kirill Kurdyukov
 */
public class Application {

    private static final String CONNECTION_STRING = "grpc://localhost:2136/local";

    public static void main(String[] args) throws InterruptedException {
        try (GrpcTransport grpcTransport = GrpcTransport.forConnectionString(CONNECTION_STRING).build();
             QueryClient queryClient = QueryClient.newClient(grpcTransport).build();
             TopicClient topicClient = TopicClient.newClient(grpcTransport).build()) {
            var ticketsYdbRepository = new TicketsYdbRepository(SessionRetryContext.create(queryClient).build());

            ticketsYdbRepository.createSchema();

            ticketsYdbRepository.addedTittle("Ticket 1", "Author 1");
            ticketsYdbRepository.addedTittle("Ticket 2", "Author 2");
            ticketsYdbRepository.addedTittle("Ticket 3", "Author 3");

            var allTickets = ticketsYdbRepository.findAll();

            System.out.println("Print all tickets: ");
            for (var ticket : allTickets) {
                printTitle(ticket);
            }

            System.out.println("Update status all tickets: NULL -> OPEN ");
            for (var ticket : allTickets) {
                ticketsYdbRepository.updateStatus(ticket.id, "OPEN");
            }

            var writer = topicClient.createSyncWriter(
                    WriterSettings.newBuilder()
                            .setProducerId("producer-task_status")
                            .setTopicPath("task_status")
                            .build()
            );

            var reader = topicClient.createSyncReader(
                    ReaderSettings.newBuilder()
                            .setConsumerName("email")
                            .setTopics(List.of(TopicReadSettings.newBuilder().setPath("task_status").build()))
                            .build()
            );

            writer.init();
            reader.init();

            var stopped_process = new AtomicBoolean();

            var writerJob = CompletableFuture.runAsync(
                    () -> {
                        System.out.println("Started write worker!");

                        while (!stopped_process.get()) {
                            for (var request : ticketsYdbRepository.getUpdatesStatusRequests()) {
                                writer.send(Message.newBuilder()
                                        .setSeqNo(request.id) // exactly once
                                        .setData(
                                                ("[" + request.uuid + " : " + request.status + "]")
                                                        .getBytes(StandardCharsets.UTF_8)
                                        )
                                        .build()
                                );
                            }
                        }

                        System.out.println("Stopped write worker!");
                    }
            );

            var readerJob = CompletableFuture.runAsync(
                    () -> {
                        System.out.println("Started read worker!");

                        while (!stopped_process.get()) {
                            try {
                                var message = reader.receive(1, TimeUnit.SECONDS);

                                if (message == null) {
                                    continue;
                                }

                                System.out.println("Received message: " + new String(message.getData()));
                            } catch (Exception e) {
                                // Ignored
                            }
                        }

                        System.out.println("Stopped read worker!");
                    }
            );

            System.out.println("Update status all tickets: OPEN -> IN_PROGRESS ");
            for (var ticket : allTickets) {
                ticketsYdbRepository.updateStatus(ticket.id, "IN_PROGRESS");
            }

            Thread.sleep(5_000);

            stopped_process.set(true);

            writerJob.join();
            readerJob.join();

            System.out.println("Print all tickets: ");
            for (var ticket : ticketsYdbRepository.findAll()) {
                printTitle(ticket);
            }

            ticketsYdbRepository.dropSchema();
        }
    }

    private static void printTitle(Title ticket) {
        System.out.println("Ticket: {id: " + ticket.id() + ", title: " + ticket.title() + ", timestamp: "
                + ticket.now() + ", author: " + ticket.author + ", link_count: "
                + ticket.linkCount + ", status: " + ticket.status + "}");
    }

    public static class TicketsYdbRepository {
        private final SessionRetryContext retryCtx;

        public TicketsYdbRepository(SessionRetryContext retryCtx) {
            this.retryCtx = retryCtx;
        }

        public void updateStatus(UUID id, String status) {
            retryCtx.supplyResult(
                    session -> session.createQuery(
                            """
                                    DECLARE $id AS UUID;
                                    DECLARE $new_status AS Text;
                                                                        
                                    UPDATE issues SET status = $new_status WHERE id = $id;
                                                                        
                                    INSERT INTO update_status_requests(id_issue, status) VALUES ($id, $new_status);
                                    """,
                            TxMode.SERIALIZABLE_RW,
                            Params.of("$id", PrimitiveValue.newUuid(id),
                                    "$new_status", PrimitiveValue.newText(status))
                    ).execute()
            ).join().getStatus().expectSuccess();
        }

        public void createSchema() {
            retryCtx.supplyResult(
                    session -> session.createQuery(
                            """
                                    CREATE TABLE issues (
                                        id UUID NOT NULL,
                                        title Text,
                                        created_at Timestamp,
                                        author Text,
                                        PRIMARY KEY (id)
                                    );
                                    """, TxMode.NONE
                    ).execute()
            ).join().getStatus().expectSuccess("Can't create schema");

            retryCtx.supplyResult(
                    session -> session.createQuery(
                            """
                                    ALTER TABLE issues ADD INDEX authorIndex GLOBAL ON (author);
                                    """, TxMode.NONE
                    ).execute()
            ).join().getStatus().expectSuccess("Can't create schema");

            retryCtx.supplyResult(
                    session -> session.createQuery(
                            """
                                    ALTER TABLE issues ADD COLUMN link_count Int64;
                                    CREATE TABLE links (
                                        source UUID NOT NULL,
                                        destination UUID NOT NULL,
                                        PRIMARY KEY(source, destination)
                                    );
                                    """, TxMode.NONE
                    ).execute()
            ).join().getStatus().expectSuccess("Can't create schema");

            retryCtx.supplyResult(
                    session -> session.createQuery(
                            """
                                    CREATE TOPIC task_status(
                                        CONSUMER email
                                    ) WITH(
                                        auto_partitioning_strategy='scale_up',
                                        min_active_partitions=2,
                                        max_active_partitions=10,
                                        retention_period = Interval('P3D')
                                    );
                                                                        
                                    ALTER TABLE issues ADD COLUMN status Text;
                                                                        
                                    CREATE TABLE update_status_requests (
                                        id serial,
                                        id_issue UUID,
                                        status Text,
                                        PRIMARY KEY (id)
                                    );
                                    """, TxMode.NONE
                    ).execute()
            ).join().getStatus().expectSuccess("Can't create schema");
        }

        public void dropSchema() {
            retryCtx.supplyResult(
                    session -> session.createQuery(
                            """
                                    DROP TABLE issues;
                                    DROP TABLE links;
                                    DROP TOPIC task_status;
                                    DROP TABLE update_status_requests;
                                    """, TxMode.NONE
                    ).execute()
            ).join().getStatus().expectSuccess("Can't drop table issues");
        }

        public void addedTittle(String title, String author) {
            var id = UUID.randomUUID();
            var now = Instant.now();

            retryCtx.supplyResult(
                    session -> session.createQuery(
                            """
                                    DECLARE $id AS UUID;
                                    DECLARE $title AS Text;
                                    DECLARE $created_at AS Timestamp;
                                    DECLARE $author AS Text;
                                    UPSERT INTO issues (id, title, created_at, author, link_count)
                                    VALUES ($id, $title, $created_at, $author, 0);
                                    """,
                            TxMode.SERIALIZABLE_RW,
                            Params.of(
                                    "$id", PrimitiveValue.newUuid(id),
                                    "$title", PrimitiveValue.newText(title),
                                    "$created_at", PrimitiveValue.newTimestamp(now),
                                    "$author", PrimitiveValue.newText(author)
                            )
                    ).execute()
            ).join().getStatus().expectSuccess("Failed upsert title");
        }

        public List<Title> findAll() {
            var titles = new ArrayList<Title>();
            var resultSet = retryCtx.supplyResult(session -> QueryReader.readFrom(
                    session.createQuery("SELECT id, title, created_at, author, link_count, status FROM issues;",
                            TxMode.SERIALIZABLE_RW)
            )).join().getValue();

            var resultSetReader = resultSet.getResultSet(0);

            while (resultSetReader.next()) {
                titles.add(new Title(
                        resultSetReader.getColumn(0).getUuid(),
                        resultSetReader.getColumn(1).getText(),
                        resultSetReader.getColumn(2).getTimestamp(),
                        resultSetReader.getColumn(3).getText(),
                        resultSetReader.getColumn(4).getInt64(),
                        resultSetReader.getColumn(5).getText()
                ));
            }

            return titles;
        }

        public List<Request> getUpdatesStatusRequests() {
            var requests = new ArrayList<Request>();
            var resultSet = retryCtx.supplyResult(session -> QueryReader.readFrom(
                    session.createQuery("SELECT id, id_issue, status FROM update_status_requests ORDER BY id LIMIT 10;",
                            TxMode.SERIALIZABLE_RW)
            )).join().getValue();

            var resultSetReader = resultSet.getResultSet(0);

            while (resultSetReader.next()) {
                requests.add(new Request(
                        resultSetReader.getColumn(0).getInt32(),
                        resultSetReader.getColumn(1).getUuid(),
                        resultSetReader.getColumn(2).getText()
                ));
            }

            return requests;
        }
    }

    public record Title(UUID id, String title, Instant now, String author, long linkCount, String status) {
    }

    public record Request(int id, UUID uuid, String status) {
    }
}
