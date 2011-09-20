// Copyright (c) 2004-2010 Azavea, Inc.
// 
// Permission is hereby granted, free of charge, to any person
// obtaining a copy of this software and associated documentation
// files (the "Software"), to deal in the Software without
// restriction, including without limitation the rights to use,
// copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following
// conditions:
// 
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
// OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
// HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.

using System;
using Azavea.Open.DAO.SQL;

namespace Azavea.Open.DAO.OleDb

{
    /// <summary>
    /// Implements a FastDao layer customized for PostGreSQL (optionally with PostGIS installed).
    /// </summary>
    public class OleDbOracleDaLayer : SqlDaDdlLayer
    {
        /// <summary>
        /// Construct the layer.  Should typically be called only by the appropriate
        /// ConnectionDescriptor.
        /// </summary>
        /// <param name="connDesc">Connection to the Firebird DB we'll be using.</param>
        public OleDbOracleDaLayer(OleDbDescriptor connDesc)
            : base(connDesc, true) { }

        #region Implementation of IDaDdlLayer

        /// <summary>
        /// Returns the DDL for the type of an automatically incrementing column.
        /// Some databases only store autonums in one col type so baseType may be
        /// ignored.
        /// </summary>
        /// <param name="baseType">The data type of the column (nominally).</param>
        /// <returns>The autonumber definition string.</returns>
        protected override string GetAutoType(Type baseType)
        {
            throw new NotSupportedException("Oracle does not have autonumbers, use a sequence.");
        }

        /// <summary>
        /// Returns the SQL type used to store a byte array in the DB.
        /// </summary>
        protected override string GetByteArrayType()
        {
            return "BLOB";
        }

        /// <summary>
        /// Returns the SQL type used to store a long in the DB.
        /// </summary>
        protected override string GetLongType()
        {
            return "BIGINT";
        }

        /// <summary>
        /// Returns the SQL type used to store a "normal" (unicode) string in the DB.
        /// </summary>
        protected override string GetStringType()
        {
            return "VARCHAR2(2000)";
        }
        /// <summary>
        /// Oracle doesn't seem to have a varchar type that is limited to ASCII characters.
        /// </summary>
        /// <returns></returns>
        protected override string GetAsciiStringType()
        {
            return "VARCHAR2(2000)";
        }

        /// <summary>
        /// Returns whether a sequence with this name exists or not.
        /// Firebird doesn't appear to support the SQL standard information_schema.
        /// </summary>
        /// <param name="name">Name of the sequence to check for.</param>
        /// <returns>Whether a sequence with this name exists in the data source.</returns>
        public override bool SequenceExists(string name)
        {
            int count = SqlConnectionUtilities.XSafeIntQuery(_connDesc,
                "SELECT count(*) FROM user_sequences WHERE sequence_name = '" +
                name.ToUpper() + "'", null);
            return count > 0;
        }

        /// <summary>
        /// Returns true if you need to call "CreateStoreRoom" before storing any
        /// data.  This method is "Missing" not "Exists" because implementations that
        /// do not use a store room can return "false" from this method without
        /// breaking either a user's app or the spirit of the method.
        /// 
        /// Store room typically corresponds to "table".
        /// </summary>
        /// <returns>Returns true if you need to call "CreateStoreRoom"
        ///          before storing any data.</returns>
        public override bool StoreRoomMissing(ClassMapping mapping)
        {
            // The user_tables doesn't store names with the owner prefix, remove it
            // for the query
            var start = mapping.Table.LastIndexOf('.');

            // -1 is not found, but is not a good index, use 0
            start = start != -1 ? (start + 1) : 0;

            int count = SqlConnectionUtilities.XSafeIntQuery(_connDesc,
                "SELECT COUNT(*) FROM user_tables where table_name = '" +
                mapping.Table.Substring(start).ToUpper() + "'", null);
            return count == 0;
        }
        
        #endregion
    }
}