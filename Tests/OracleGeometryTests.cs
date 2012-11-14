using System;
using System.Collections.Generic;
using Azavea.Open.Common;
using Azavea.Open.DAO.Criteria;
using Azavea.Open.DAO.Criteria.Spatial;
using GeoAPI.Geometries;
using NetTopologySuite.Geometries;
using NUnit.Framework;

namespace Azavea.Open.DAO.OleDb.Tests
{
    /// <exclude/>
    [TestFixture]
    public class OracleGeometryTests
    {
        private Open.DAO.FastDAO<PointClass> _pointDao;
        private Open.DAO.FastDAO<LineClass> _lineDao;
        private Open.DAO.FastDAO<PolyClass> _polyDao;

        /// <exclude/>
        [TestFixtureSetUp] public void SetUp()
        {
            var config = new Config("..\\..\\Tests\\OracleDao.config", "OracleDaoConfig");
            _pointDao = new Open.DAO.FastDAO<PointClass>(config, "DAO");
            _lineDao = new Open.DAO.FastDAO<LineClass>(config, "DAO");
            _polyDao = new Open.DAO.FastDAO<PolyClass>(config, "DAO");
            //SetupPoints();
            //SetupLines();
            //SetupPolys();
        }

        /// <exclude/>
        [Test]
        public void TestGetAllPoints()
        {
            IList<PointClass> points = _pointDao.Get();
            Assert.AreEqual(10, points.Count, "Wrong number of points.");
        }

        /// <exclude/>
        [Test]
        public void TestGetAllLines()
        {
            IList<LineClass> lines = _lineDao.Get();
            Assert.AreEqual(6, lines.Count, "Wrong number of lines.");
        }

        /// <exclude/>
        [Test]
        public void TestGetAllPolys()
        {
            IList<PolyClass> polys = _polyDao.Get();
            Assert.AreEqual(6, polys.Count, "Wrong number of polygons.");
        }

        /// <exclude/>
        [Test]
        public void TestGetAllPolysSorted()
        {
            DaoCriteria crit = new DaoCriteria();
            crit.Orders.Add(new SortOrder("Int"));
            IList<PolyClass> polys = _polyDao.Get(crit);
            Assert.AreEqual(6, polys.Count, "Wrong number of polygons.");
            int lastVal = int.MinValue;
            foreach (PolyClass poly in polys)
            {
                Assert.IsTrue(lastVal <= poly.Int,
                    "This polygon's int value wasn't greater or equal to the last one: " + poly);
                lastVal = poly.Int;
            }
        }

        /// <exclude/>
        [Test]
        public void TestGetMultipleCriteria()
        {
            DaoCriteria crit = new DaoCriteria(BooleanOperator.Or);
            crit.Expressions.Add(new Open.DAO.Criteria.EqualExpression("Double", 10000));
            crit.Expressions.Add(new Open.DAO.Criteria.GreaterExpression("Int", 300));
            IList<PointClass> points = _pointDao.Get(crit);
            Assert.AreEqual(5, points.Count, "Wrong number of points.");
            IList<LineClass> lines = _lineDao.Get(crit);
            Assert.AreEqual(4, lines.Count, "Wrong number of lines.");
            IList<PolyClass> polys = _polyDao.Get(crit);
            Assert.AreEqual(4, polys.Count, "Wrong number of polygons.");
        }

        /// <exclude/>
        [Test]
        public void TestGetIntersects()
        {
            DaoCriteria crit = new DaoCriteria();
            Coordinate[] coords = new Coordinate[5];
            //coords[0] = new Coordinate(100, 100);
            //coords[1] = new Coordinate(200, 100);
            //coords[2] = new Coordinate(200, 150);
            //coords[3] = new Coordinate(100, 150);
            //coords[4] = new Coordinate(100, 100);
            coords[0] = new Coordinate(99, 99);
            coords[1] = new Coordinate(201, 99);
            coords[2] = new Coordinate(201, 151);
            coords[3] = new Coordinate(99, 151);
            coords[4] = new Coordinate(99, 99);
            IGeometry poly = new Polygon(new LinearRing(coords));
            poly.SRID = 2271;
            crit.Expressions.Add(new IntersectsExpression("Shape", poly));
            IList<PointClass> points = _pointDao.Get(crit);
            Assert.AreEqual(6, points.Count, "Wrong number of points.");
            IList<LineClass> lines = _lineDao.Get(crit);
            Assert.AreEqual(2, lines.Count, "Wrong number of lines.");
            poly.SRID = 2272;
            IList<PolyClass> polys = _polyDao.Get(crit);
            Assert.AreEqual(2, polys.Count, "Wrong number of polygons.");
        }

        /// <exclude/>
        [Test]
        public void TestGetContains()
        {
            DaoCriteria crit = new DaoCriteria();
            Coordinate[] coords = new Coordinate[5];
            //coords[0] = new Coordinate(100, 100);
            //coords[1] = new Coordinate(200, 100);
            //coords[2] = new Coordinate(200, 150);
            //coords[3] = new Coordinate(100, 150);
            //coords[4] = new Coordinate(100, 100);
            coords[0] = new Coordinate(99, 99);
            coords[1] = new Coordinate(201, 99);
            coords[2] = new Coordinate(201, 151);
            coords[3] = new Coordinate(99, 151);
            coords[4] = new Coordinate(99, 99);
            IGeometry poly = new Polygon(new LinearRing(coords));
            poly.SRID = 2271;
            crit.Expressions.Add(new WithinExpression("Shape", poly));
            IList<PointClass> points = _pointDao.Get(crit);
            Assert.AreEqual(6, points.Count, "Wrong number of points.");
            IList<LineClass> lines = _lineDao.Get(crit);
            Assert.AreEqual(1, lines.Count, "Wrong number of lines.");
            poly.SRID = 2272;
            IList<PolyClass> polys = _polyDao.Get(crit);
            Assert.AreEqual(1, polys.Count, "Wrong number of polygons.");
        }

        /// <exclude/>
        [Test]
        [Ignore]
        public void TestUpdatePoint()
        {
            PointClass obj = MakePoint(123, 456);
            int uid = (int) DateTime.Now.Ticks;
            obj.Int = uid;
            _pointDao.Insert(obj);
            // Get it fresh from SDE.
            obj = _pointDao.GetFirst("Int", uid);
            Assert.Greater(obj.ObjectID, 0, "Should have object id set.");
            string newStr = "Different string.";
            obj.String = newStr;
            _pointDao.Update(obj);
            // Get it fresh from SDE.
            obj = _pointDao.GetFirst("ObjectID", obj.ObjectID);
            Assert.AreEqual(newStr, obj.String, "String wasn't what we updated it to.");
        }

        private void SetupPoints()
        {
            _pointDao.Truncate();
            _pointDao.Insert(MakePoint(100, 100));
            _pointDao.Insert(MakePoint(110, 110));
            _pointDao.Insert(MakePoint(120, 120));
            _pointDao.Insert(MakePoint(130, 130));
            _pointDao.Insert(MakePoint(140, 140));
            _pointDao.Insert(MakePoint(150, 150));
            _pointDao.Insert(MakePoint(160, 160));
            _pointDao.Insert(MakePoint(170, 170));
            _pointDao.Insert(MakePoint(180, 180));
            _pointDao.Insert(MakePoint(190, 190));
        }

        private void SetupLines()
        {
            _lineDao.Truncate();
            _lineDao.Insert(MakeLine(100, 100));
            _lineDao.Insert(MakeLine(100, 200));
            _lineDao.Insert(MakeLine(100, 300));
            _lineDao.Insert(MakeLine(200, 100));
            _lineDao.Insert(MakeLine(200, 200));
            _lineDao.Insert(MakeLine(200, 300));
        }

        private void SetupPolys()
        {
            _polyDao.Truncate();
            _polyDao.Insert(MakePoly(100, 100));
            _polyDao.Insert(MakePoly(100, 200));
            _polyDao.Insert(MakePoly(100, 300));
            _polyDao.Insert(MakePoly(200, 100));
            _polyDao.Insert(MakePoly(200, 200));
            _polyDao.Insert(MakePoly(200, 300));
        }

        /// <exclude/>
        public PointClass MakePoint(int x, int y)
        {
            PointClass retVal = new PointClass();
            retVal.Int = x + y;
            retVal.Double = x * y;
            retVal.Shape = new Point(x, y);
            return retVal;
        }
        /// <exclude/>
        public LineClass MakeLine(int x, int y)
        {
            LineClass retVal = new LineClass();
            retVal.Int = x + y;
            retVal.Double = x * y;
            Coordinate[] coords = new Coordinate[4];
            coords[0] = new Coordinate(x, y);
            coords[1] = new Coordinate(x + 10, y);
            coords[2] = new Coordinate(x + 10, y + 20);
            coords[3] = new Coordinate(x + 20, y + 20);
            retVal.Shape = new LineString(coords);
            return retVal;
        }
        /// <exclude/>
        public PolyClass MakePoly(int x, int y)
        {
            PolyClass retVal = new PolyClass();
            retVal.Int = x + y;
            retVal.Double = x * y;
            retVal.Date = DateTime.Now;
            Coordinate[] coords = new Coordinate[5];
            coords[0] = new Coordinate(x, y);
            coords[1] = new Coordinate(x + 10, y);
            coords[2] = new Coordinate(x + 10, y + 20);
            coords[3] = new Coordinate(x, y + 20);
            coords[4] = new Coordinate(x, y);
            retVal.Shape = new Polygon(new LinearRing(coords));
            return retVal;
        }
    }

    /// <exclude/>
    public class PointClass
    {
        /// <exclude/>
        public int ObjectID;
        /// <exclude/>
        public int Int;
        /// <exclude/>
        public string String;
        /// <exclude/>
        public double Double;
        /// <exclude/>
        public IGeometry Shape;

        /// <exclude/>
        public override string ToString()
        {
            return "Point_" + ObjectID + "_" + Int + "_" + String + "_" + Double + "_" + Shape;
        }
    }
    /// <exclude/>
    public class LineClass
    {
        /// <exclude/>
        public int ObjectID;
        /// <exclude/>
        public int Int;
        /// <exclude/>
        public string String;
        /// <exclude/>
        public double Double;
        /// <exclude/>
        public IGeometry Shape;

        /// <exclude/>
        public override string ToString()
        {
            return "Line_" + ObjectID + "_" + Int + "_" + String + "_" + Double + "_" + Shape;
        }
    }
    /// <exclude/>
    public class PolyClass
    {
        /// <exclude/>
        public int ObjectID;
        /// <exclude/>
        public int Int;
        /// <exclude/>
        public string String;
        /// <exclude/>
        public double Double;
        /// <exclude/>
        public DateTime? Date;
        /// <exclude/>
        public IGeometry Shape;

        /// <exclude/>
        public override string ToString()
        {
            return "Poly_" + ObjectID + "_" + Int + "_" + String + "_" + Double + "_" + Date + "_" + Shape;
        }
    }
}
