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
using System.Collections.Generic;
using Azavea.Open.Common;
using Azavea.Open.DAO.SQL;
using Azavea.Open.DAO.Tests;
using NUnit.Framework;

namespace Azavea.Open.DAO.OleDb.Tests
{
    /// <exclude/>
    [TestFixture]
    public class OracleDaoTests : AbstractFastDAOTests
    {
        /// <exclude/>
        public OracleDaoTests()
            : base(new Config("..\\..\\Tests\\OracleDao.config", "OracleDaoConfig"), "DAO",
            false, true, false, true, false, true) { }

        /// <exclude/>
        [Test]
        public void TestGetMappingFromSchema()
        {
            SqlUtilTests.TestGetNullableTableMappingFromSchema((AbstractSqlConnectionDescriptor)
                ConnectionDescriptor.LoadFromConfig(new Config("..\\..\\Tests\\OracleDao.config", "OracleDaoConfig"), "DAO"),
                "NULLABLETABLE");
        }

        /// <exclude/>
        [Test]
        public void TestCheckStoreRoomMissing()
        {
            OleDbOracleDaLayer ddl =
                new OleDbOracleDaLayer(
                    (OleDbDescriptor)
                    ConnectionDescriptor.LoadFromConfig(
                        new Config("..\\..\\Tests\\OracleDao.config", "OracleDaoConfig"), "DAO"));

            // Make sure the store room to test does not exist
            var colDef = new ClassMapColDefinition("Id", "Id", "INTEGER");
            var cm = new ClassMapping("test", "UNITTEST.StoreroomDoesNotExist", new List<ClassMapColDefinition>{colDef}, false);
            ddl.DeleteStoreRoom(cm);

            try
            {
                // Hasn't been added yet, should not exist
                Assert.IsTrue(
                    ddl.StoreRoomMissing(cm), "Oracle storeroom should not exist");

                ddl.CreateStoreRoom(cm);

                // Correct name, should exist
                Assert.IsFalse(
                    ddl.StoreRoomMissing(cm), "Oracle Storeroom should exist");
            }
            finally
            {
                // Delete the storeromm, no matter what
                ddl.DeleteStoreRoom(cm);
            }

            // Storeroom should be gone
            Assert.IsTrue(
                ddl.StoreRoomMissing(cm), "Oracle storeroom should not exist");

        }
    }
}