using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;

namespace Woof.DataEx {

    /// <summary>
    /// Minimalistic PostgreSQL backend.
    /// </summary>
    class PgSql : IDbSource {

        #region Configuration

        /// <summary>
        /// Connection string for PostgreSQL database access.
        /// </summary>
        private readonly string ConnectionString;

        public PgSql(string connectionString) => ConnectionString = connectionString;

        /// <summary>
        /// Creates new PostgreSQL input parameter.
        /// </summary>
        /// <param name="name">Parameter name.</param>
        /// <param name="value">Parameter value.</param>
        /// <returns><see cref="NpgsqlParameter"/>.</returns>
        public DbParameter I(string name, object value) => new NpgsqlParameter(name, value);

        /// <summary>
        /// Creates new PostgreSQL input / output parameter.
        /// </summary>
        /// <param name="name">Parameter name.</param>
        /// <param name="value">Parameter value.</param>
        /// <returns><see cref="NpgsqlParameter"/>.</returns>
        public DbParameter IO(string name, object value) => new NpgsqlParameter(name, value) { Direction = ParameterDirection.InputOutput };

        /// <summary>
        /// Creates new PostgreSQL output parameter.
        /// </summary>
        /// <param name="name">Parameter name.</param>
        /// <returns><see cref="NpgsqlParameter"/>.</returns>
        public DbParameter O(string name) => new NpgsqlParameter(name, null) { Direction = ParameterDirection.Output };

        

        #endregion

        #region Command builder

        /// <summary>
        /// Creates new <see cref="NpgsqlCommand"/> from stored procedure name and optional parameters and opens a connection if needed.
        /// </summary>
        /// <param name="connection">Disposable connection.</param>
        /// <param name="procedure">Stored procedure name.</param>
        /// <param name="parameters">Optional PostgreSQL parameters.</param>
        /// <returns>Command.</returns>
        private async Task<NpgsqlCommand> GetCommandAsync(NpgsqlConnection connection, string procedure, params DbParameter[] parameters) {
            var command = new NpgsqlCommand(procedure, connection) { CommandType = CommandType.StoredProcedure };
            if (parameters.Length > 0) foreach (var parameter in parameters) command.Parameters.Add(parameter);
            if (connection.State != ConnectionState.Open) await connection.OpenAsync();
            return command;
        }

        #endregion

        #region Data operations

        /// <summary>
        /// Executes a stored procedure asynchronously and returns number of rows affected.
        /// </summary>
        /// <param name="procedure">Stored procedure name.</param>
        /// <param name="parameters">Optional <see cref="NpgsqlParameter"/> parameters.</param>
        /// <returns>-1, due to the driver lack of correct implementation.</returns>
        public async Task<int> ExecuteAsync(string procedure, params DbParameter[] parameters) {
            using (var connection = new NpgsqlConnection(ConnectionString))
            using (var command = await GetCommandAsync(connection, procedure, parameters)) return await command.ExecuteNonQueryAsync();
        }

        /// <summary>
        /// Executes a stored procedure asynchronously and returns a scalar.
        /// </summary>
        /// <param name="procedure">Stored procedure name.</param>
        /// <param name="parameters">Optional <see cref="NpgsqlParameter"/> parameters.</param>
        /// <returns>Scalar object.</returns>
        public async Task<T> GetScalarAsync<T>(string procedure, params DbParameter[] parameters) {
            object value = null;
            using (var connection = new NpgsqlConnection(ConnectionString))
            using (var command = await GetCommandAsync(connection, procedure, parameters)) value = await command.ExecuteScalarAsync();
            if (value == null || value is DBNull) return default(T);
            else return (T)value;
        }

        /// <summary>
        /// Executes a stored procedure asynchronously and returns a dataset.
        /// </summary>
        /// <param name="procedure">Stored procedure name.</param>
        /// <param name="parameters">Optional <see cref="NpgsqlParameter"/> parameters.</param>
        /// <returns>Table data.</returns>
        public async Task<object[][]> GetTableAsync(string procedure, params DbParameter[] parameters) {
            var table = new List<object[]>();
            using (var connection = new NpgsqlConnection(ConnectionString))
            using (var command = await GetCommandAsync(connection, procedure, parameters)) {
                using (var reader = await command.ExecuteReaderAsync()) {
                    while (await reader.ReadAsync()) {
                        object[] values = new object[reader.FieldCount];
                        reader.GetValues(values);
                        table.Add(values);
                    }
                }
            }
            return table.ToArray();
        }

        /// <summary>
        /// Executes a stored procedure asynchronously and returns a dataset.
        /// </summary>
        /// <typeparam name="T">Record type.</typeparam>
        /// <param name="procedure">Stored procedure name.</param>
        /// <param name="parameters">Optional <see cref="NpgsqlParameter"/> parameters.</param>
        /// <returns>Table data.</returns>
        public async Task<T[]> GetTableAsync<T>(string procedure, params DbParameter[] parameters) where T : new()
            => (await GetTableAsync(procedure, parameters)).AsArrayOf<T>();

        /// <summary>
        /// Executes a stored procedure asynchronously and returns a record.
        /// </summary>
        /// <typeparam name="T">Record type.</typeparam>
        /// <param name="procedure">Stored procedure name.</param>
        /// <param name="parameters">Optional <see cref="NpgsqlParameter"/> parameters.</param>
        /// <returns>Table data.</returns>
        public async Task<T> GetRecordAsync<T>(string procedure, params DbParameter[] parameters) where T : new() {
            var raw = await GetTableAsync(procedure, parameters);
            var row = raw?.FirstOrDefault();
            return row.As<T>();
        }

        /// <summary>
        /// Executes a stored procedure asynchronously and returns multiple datasets.
        /// </summary>
        /// <param name="procedure">Stored procedure name.</param>
        /// <param name="parameters">Optional <see cref="NpgsqlParameter"/> parameters.</param>
        /// <returns>An array of table data.</returns>
        public async Task<object[][][]> GetDataAsync(string procedure, params DbParameter[] parameters) {
            var data = new List<object[][]>();
            var table = new List<object[]>();
            using (var connection = new NpgsqlConnection(ConnectionString))
            using (var command = await GetCommandAsync(connection, procedure, parameters)) {
                using (var reader = await command.ExecuteReaderAsync()) {
                    do {
                        while (await reader.ReadAsync()) {
                            object[] values = new object[reader.FieldCount];
                            reader.GetValues(values);
                            table.Add(values);
                        }
                        data.Add(table.ToArray());
                        table.Clear();
                    } while (await reader.NextResultAsync());
                }
            }
            return data.ToArray();
        }

        

        #endregion

    }

}
