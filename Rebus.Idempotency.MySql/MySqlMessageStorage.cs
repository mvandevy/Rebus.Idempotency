using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.Idempotency.MySql.Extensions;
using System.Collections.Generic;

namespace Rebus.Idempotency.MySql
{
    public class MySqlMessageStorage : IMessageStorage, IDisposable
    {
        private const int OperationCancelledNumber = 3980;
        private readonly MySqlConnectionHelper _connectionHelper;
        private readonly string _dataTableName;
        private readonly ILog _log;
        private readonly IdempotencyDataSerializer _serializer;
        private bool _disposed;

        public MySqlMessageStorage(MySqlConnectionHelper connectionHelper, string dataTableName,
            IRebusLoggerFactory rebusLoggerFactory)
        {
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            _connectionHelper = connectionHelper ?? throw new ArgumentNullException(nameof(connectionHelper));
            _dataTableName = dataTableName ?? throw new ArgumentNullException(nameof(dataTableName));
            _log = rebusLoggerFactory.GetLogger<MySqlMessageStorage>();
            _serializer = new IdempotencyDataSerializer();
        }

        public async Task<MessageData> Find(MessageId messageId)
        {
            using (var connection = await _connectionHelper.GetConnection())
            {
                MessageData msgData;

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"
                        SELECT s.`message_id`, s.`defer_count`, s.`input_queue_address`, s.`processing_thread_id`, s.`time_thread_id_assigned`, `data`
                            FROM `{_dataTableName}` s
                            WHERE s.`message_id` = @message_id and s.`defer_count`= @defer_count
                        ";
                    command.Parameters.Add(command.CreateParameter("message_id", DbType.String, messageId.OriginalMessageId));
                    command.Parameters.Add(command.CreateParameter("defer_count", DbType.Byte, messageId.DeferCount));

                    try
                    {
                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            if (!await reader.ReadAsync()) return null;

                            var msgId = (Guid)reader.ExtractValue("message_id");
                            var deferCount = (byte)reader.ExtractValue("defer_count");
                            var inputQueueAddress = (string)reader.ExtractValue("input_queue_address");
                            var processingThreadId = (int?)reader.ExtractValue("processing_thread_id");
                            var timeThreadIdAssigned = (DateTime?)reader.ExtractValue("time_thread_id_assigned");
                            var idempotencyData = _serializer.DeserializeData((string)(reader.ExtractValue("data")));

                            msgData = MessageDataFactory.BuildMessageData(msgId, deferCount, inputQueueAddress,
                                processingThreadId, timeThreadIdAssigned);
                            msgData.IdempotencyData = idempotencyData;
                        }
                    }
                    catch (SqlException sqlException) when (sqlException.Number == OperationCancelledNumber)
                    {
                        // ADO.NET does not throw the right exception when the task gets cancelled - therefore we need to do this:
                        throw new TaskCanceledException("Receive operation was cancelled", sqlException);
                    }
                    finally
                    {
                        connection.Complete();
                    }
                }

                return msgData;
            }
        }

        public async Task<bool> IsProcessing(MessageId messageId)
        {
            using (var connection = await _connectionHelper.GetConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"
                        SELECT s.`processing_thread_id`
                            FROM `{_dataTableName}` s
                            WHERE s.`message_id` = @message_id and s.`defer_count` = @defer_count
                        ";
                    command.Parameters.Add(command.CreateParameter("message_id", DbType.String, messageId.OriginalMessageId));
                    command.Parameters.Add(command.CreateParameter("defer_count", DbType.Byte, messageId.DeferCount));

                    try
                    {
                        var processingThreadId = await command.ExecuteScalarAsync();
                        if (processingThreadId is DBNull) return false;
                        return processingThreadId != null;
                    }
                    catch (SqlException sqlException) when (sqlException.Number == OperationCancelledNumber)
                    {
                        // ADO.NET does not throw the right exception when the task gets cancelled - therefore we need to do this:
                        throw new TaskCanceledException("Receive operation was cancelled", sqlException);
                    }
                    finally
                    {
                        connection.Complete();
                    }
                }
            }
        }

        public async Task InsertOrUpdate(MessageData messageData)
        {
            if (messageData == null)
            {
                return;
            }

            using (var connection = await _connectionHelper.GetConnection())
            {
                // first, delete existing index
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"

                        INSERT INTO {_dataTableName} (`message_id`, `defer_count`, `input_queue_address`, `processing_thread_id`, `time_thread_id_assigned`, `data`)
                        VALUES (@message_id, @defer_count, @input_queue_address, @processing_thread_id, @time_thread_id_assigned, @data)
                        ON DUPLICATE KEY UPDATE 
                            `message_id` = @message_id, 
                            `defer_count` = @defer_count, 
                            `input_queue_address` = @input_queue_address, 
                            `processing_thread_id` = @processing_thread_id, 
                            `time_thread_id_assigned` = @time_thread_id_assigned,
                            `data` = @data;

                        ";
                    command.Parameters.Add(command.CreateParameter("message_id", DbType.String, messageData.MessageId.OriginalMessageId));
                    command.Parameters.Add(command.CreateParameter("defer_count", DbType.Byte, messageData.MessageId.DeferCount));
                    command.Parameters.Add(command.CreateParameter("input_queue_address", DbType.String,
                        messageData.InputQueueAddress));
                    command.Parameters.Add(command.CreateParameter("processing_thread_id", DbType.Int32,
                        messageData.ProcessingThreadId));
                    command.Parameters.Add(command.CreateParameter("time_thread_id_assigned", DbType.DateTime,
                        messageData.TimeThreadIdAssigned));
                    command.Parameters.Add(command.CreateParameter("data", DbType.String,
                         _serializer.SerializeData(messageData.IdempotencyData)));
                    await command.ExecuteNonQueryAsync();
                }

                connection.Complete();
            }
        }

        public async Task Verify()
        {
            using (var connection = await _connectionHelper.GetConnection())
            {
                var tableNames = new List<string>();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "select * from information_schema.tables where table_name = @table_name";
                    command.Parameters.Add(command.CreateParameter("table_name", DbType.String, _dataTableName));
                    
                    var result = await command.ExecuteScalarAsync();
                    if(result is DBNull || result == null)
                        throw new Exception($"Table {_dataTableName} does not exist!");
                }

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $@"
                            SELECT COLUMN_NAME, DATA_TYPE
                            FROM INFORMATION_SCHEMA.COLUMNS
                            WHERE TABLE_NAME = @table_name
                        ;";

                    command.Parameters.Add(command.CreateParameter("table_name", DbType.String, _dataTableName));

                    var columns = new List<(string columnName,string dataType)>();
                    var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        columns.Add((reader[0].ToString().ToLowerInvariant(), reader[1].ToString().ToLowerInvariant()));
                    }

                    if(!columns.Exists(x => x.columnName == "defer_count" && x.dataType == "tinyint")) 
                    {
                        throw new Exception("The defer_count column is required to be tinyint for this package version. " +
                            $"Please update the database schema for your idempotency table: {_dataTableName}.");
                    }
                }
            }
        }

        public async Task EnsureTablesAreCreated()
        {
            using (var connection = await _connectionHelper.GetConnection())
            {
                var tableNames = connection.GetTableNames().ToHashSet();

                var hasDataTable = tableNames.Contains(_dataTableName);

                if (hasDataTable)
                {
                    return;
                }

                _log.Info($"Message idempotency tables '{_dataTableName}' (data) do not exist - they will be created now");

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $@"
                            CREATE TABLE `{_dataTableName}` (
                                `message_id` CHAR(36) NOT NULL,
                                `defer_count` TINYINT(3) UNSIGNED NOT NULL DEFAULT 0,
                                `input_queue_address` VARCHAR(200) CHARACTER SET UTF8 NOT NULL,
                                `processing_thread_id` INT NULL,
                                `time_thread_id_assigned` TIMESTAMP NULL,
                                `data` MEDIUMTEXT NULL,
                                PRIMARY KEY (`message_id`, `defer_count`)
                            );";

                    command.ExecuteNonQuery();
                }

                connection.Complete();
            }
        }

        public async Task Cleanup(TimeSpan olderThan)
        {
            if (olderThan.TotalSeconds <= 0) return;

            using (var connection = await _connectionHelper.GetConnection())
            {
                _log.Info($"Cleaning up idempotency message table '{_dataTableName}'. Removing items older than {olderThan.TotalMinutes} minutes.");

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $@"
                            DELETE FROM {_dataTableName} 
                            WHERE `time_thread_id_assigned` < TIMESTAMPADD(MINUTE,{olderThan.TotalMinutes},NOW());
                        ";

                    command.ExecuteNonQuery();
                }

                connection.Complete();
            }
        }

        public void Dispose()
        {
            if (_disposed) return;

            try
            {
            }
            finally
            {
                _disposed = true;
            }
        }


    }
}
