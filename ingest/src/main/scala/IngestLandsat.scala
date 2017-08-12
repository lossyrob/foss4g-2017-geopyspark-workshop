package ingest

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.reader.TiffTagsReader
import geotrellis.raster.resample.Bilinear
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.kryo._
import geotrellis.spark.io.file._
import geotrellis.spark.io.s3._
import geotrellis.spark.io.s3.util.S3RangeReader
import geotrellis.spark.io.index._
import geotrellis.spark.io.avro.codecs._
import geotrellis.spark.tiling._
import geotrellis.spark.pyramid._
import geotrellis.util.StreamingByteReader
import geotrellis.vector._
import geotrellis.vector.io._
import com.amazonaws.services.s3.AmazonS3URI
import com.amazonaws.services.s3.model.AmazonS3Exception
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.serializer.KryoSerializer

import java.io._
import java.net.URI
import java.time._
import java.time.format._

import spray.json._

case class L8Scene(time: ZonedDateTime, sceneId: String, band: Int, path: String) {
  def instant = time.toInstant.toEpochMilli
  def uri = new URI(path)
  lazy val (bucket, prefix) = {
    val s3Uri = new AmazonS3URI(path)
    val prefix =
      Option(s3Uri.getKey) match {
        case Some(s) => s
        case None => ""
      }
    (s3Uri.getBucket, prefix)
  }
}

object IngestLandsat {
  val validBounds = """
{
        "type": "Polygon",
        "coordinates": [
          [
            [
              -130.60546875,
              21.94304553343818
            ],
            [
              -63.54492187500001,
              21.94304553343818
            ],
            [
              -63.54492187500001,
              51.72702815704774
            ],
            [
              -130.60546875,
              51.72702815704774
            ],
            [
              -130.60546875,
              21.94304553343818
            ]
          ]
        ]
      }""".parseGeoJson[Polygon].reproject(LatLng, WebMercator)

  object JarResource {
    def apply(name: String): String = {
      val stream: InputStream = getClass.getResourceAsStream(s"/$name")
      try { scala.io.Source.fromInputStream( stream ).getLines.mkString(" ") } finally { stream.close() }
    }
  }

  def main(args: Array[String]): Unit = {
    val numPartitions =
      args(0).toInt

    val month =
      if(args.length > 1) {
        if(args(1) != "local") {
          Some(args(1).toInt)
        } else {
          None
        }
      }

    val layerName = "landsat-8-continental-us-2016"
    val layoutScheme = ZoomedLayoutScheme(WebMercator)

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    // Manually create KeyIndex that covers the space of all updates
    val keyIndex = ZCurveKeyIndexMethod.byDay().createIndex(
      KeyBounds(
        SpaceTimeKey(0, 0, ZonedDateTime.of(2000, 1, 1, 1, 1, 1, 1, ZoneId.of("Z"))),
        SpaceTimeKey(Int.MaxValue, Int.MaxValue, ZonedDateTime.of(2050, 1, 1, 1, 1, 1, 1, ZoneId.of("Z")))
      )
    )

    val conf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", "Landsat Ingest")
      .set("spark.default.parallelism", s"$numPartitions")
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)
      .set("spark.network.timeout", "800")


    implicit val sc = new SparkContext(conf)

    val (writer, updater, as, csvName) =
      if(args.contains("local")) {
        val writer = FileLayerWriter("/Users/rob/proj/presentations/2017-foss4g-workshop/notebooks/catalog")
        val updater = FileLayerUpdater("/Users/rob/proj/presentations/2017-foss4g-workshop/notebooks/catalog")
        val as = writer.attributeStore

        (writer, updater, as, "l8-scenes-ri.csv")
      } else {
        val writer = S3LayerWriter("datahub-catalogs-us-east-1", "")
        val updater = S3LayerUpdater("datahub-catalogs-us-east-1", "")
        val as = writer.attributeStore

        (writer, updater, as, "l8-scenes.csv")
      }

    try {
      // Read in csv
      val stream: InputStream = getClass.getResourceAsStream(s"/${csvName}")
      val scenes =
        try {
          val full =
            scala.io.Source.fromInputStream(stream).getLines
              .drop(1)
              .map { line =>
                val values = line.split(',').map(_.replace("\"", "")).toVector
                val time =
                  LocalDateTime.parse(values(0), formatter).atOffset(ZoneOffset.UTC).toZonedDateTime

                L8Scene(time, values(1), values(2).toInt, values(3))
              }
              .toList

          val filter = { scenes: List[L8Scene] =>
            val lf =
              if(args.contains("local")) { full.take(5*10) }
              else { full }

            month match {
              case Some(m) => lf.filter(_.time.getMonthValue == m)
              case None => lf
            }
          }

          filter(full)
        } finally { stream.close() }

      println(s"SCENES: ${scenes.size}")

      scenes
        .groupBy(_.time.getDayOfWeek)
        .foreach { case (day, dayScenes) =>
          println(s"INGESTING DAY ${day} MONTH ${month}")
          val initialPartitions = if(dayScenes.size > 100) { 10000 } else { 50 }
          val sceneRdd = sc.parallelize(dayScenes, initialPartitions)

          val mapTransform = layoutScheme.levelForZoom(13).layout.mapTransform

          val source: RDD[(TemporalProjectedExtent, MultibandTile)] =
            sceneRdd
              .flatMap { scene =>
                val readerOpt =
                  try {
                    Some(StreamingByteReader(S3RangeReader(scene.uri)))
                  } catch {
                    case e: AmazonS3Exception =>
                      // Some images are listed but don't exist
                      None
                  }
                readerOpt match {
                  case Some(reader) =>
                    val tiffTags = TiffTagsReader.read(reader)
                    if(validBounds.contains(tiffTags.extent.reproject(tiffTags.crs, WebMercator))) {
                      RasterReader.listWindows(tiffTags.cols, tiffTags.rows, Some(512))
                        .map { gridBounds =>
                          (scene, gridBounds, (tiffTags.cols, tiffTags.rows))
                        }
                    } else {
                      Array[(L8Scene, GridBounds, (Int, Int))]()
                    }
                  case None => Array[(L8Scene, GridBounds, (Int, Int))]()
                }
              }
              .repartition(numPartitions)
              .map { case (scene, gridBounds, (totalCols, totalRows)) =>
                // Buffer the pixel window to account for reprojection
                val bufferedGridBounds =
                  GridBounds(
                    math.max(0, gridBounds.colMin - 3),
                    math.max(0, gridBounds.rowMin - 3),
                    math.min(totalCols - 1, gridBounds.colMax + 3),
                    math.min(totalRows - 1, gridBounds.rowMax + 3)
                  )
                val reader = StreamingByteReader(S3RangeReader(scene.uri))
                val geotiff = SinglebandGeoTiff.streaming(reader)
                val raster: Raster[Tile] =
                  geotiff.raster.crop(bufferedGridBounds).reproject(geotiff.crs, WebMercator, Bilinear)
                  val projectedExtent = ProjectedExtent(raster.extent, WebMercator)
                  val tpe = TemporalProjectedExtent(projectedExtent, scene.time)
                  val tile = raster.tile
                  ((scene.sceneId, (gridBounds.colMin, gridBounds.rowMin)), (scene.band, tpe, tile))
              }
              .groupByKey(new HashPartitioner(numPartitions))
              .flatMap { case (_, tiles) =>
                val tileArr = tiles.toArray
                if(tileArr.size != 5) { None }
                else {
                  val (_, tpe, _) = tileArr(0)

                  val tile =
                    MultibandTile(
                      tileArr
                        .sortBy { case (band, tpe, tile) => band }
                        .map(_._3)
                    )
                    Some((tpe, tile))
                }
              }

          val (zoom, md) = {
            val (zoom, md) = source.collectMetadata[SpaceTimeKey](layoutScheme)
            println(md.toJson.prettyPrint)
            (zoom, md.copy(cellType = UShortConstantNoDataCellType))
          }

          val tiles = ContextRDD(source.tileToLayout[SpaceTimeKey](md), md)

          //      Pyramid.levelStream(tiles, layoutScheme, zoom, 0, Bilinear).foreach { case (z, layer) =>
          val (z, layer) = (zoom, tiles)
          //{
            val lid = LayerId(layerName, z)
            if(as.layerExists(lid)) {
              // Some manual updating to speed things up
              // Taken from S3LayerUpdater
              val LayerAttributes(header, metadata, keyIndex, writerSchema) =
                as.readLayerAttributes[S3LayerHeader, TileLayerMetadata[SpaceTimeKey], SpaceTimeKey](lid)
              val prefix = header.key
              val bucket = header.bucket
              val maxWidth = Index.digits(keyIndex.toIndex(keyIndex.keyBounds.maxKey))
              val keyPath = (key: SpaceTimeKey) => makePath(prefix, Index.encode(keyIndex.toIndex(key), maxWidth))
              val updatedMetadata = metadata.merge(layer.metadata)
              val codec  = KeyValueRecordCodec[SpaceTimeKey, MultibandTile]
              val schema = codec.schema
              as.writeLayerAttributes(lid, header, updatedMetadata, keyIndex, schema)
              S3RDDWriter.write(layer, bucket, keyPath)
            } else {
              writer.write(lid, layer, keyIndex)
            }
            //}
      }
    } finally {
      sc.stop()
    }
  }

  // def main2(args: Array[String]): Unit = {
  //   val numPartitions =
  //     args(0).toInt

  //   val layerName = "landsat-8-continental-us-2016"
  //   val layoutScheme = ZoomedLayoutScheme(WebMercator)

  //   val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  //   // Manually create KeyIndex that covers the space of all updates
  //   val keyIndex = ZCurveKeyIndexMethod.byDay().createIndex(
  //     KeyBounds(
  //       SpaceTimeKey(0, 0, ZonedDateTime.of(2000, 1, 1, 1, 1, 1, 1, ZoneId.of("Z"))),
  //       SpaceTimeKey(Int.MaxValue, Int.MaxValue, ZonedDateTime.of(2050, 1, 1, 1, 1, 1, 1, ZoneId.of("Z")))
  //     )
  //   )

  //   val conf = new SparkConf()
  //     .setIfMissing("spark.master", "local[*]")
  //     .setIfMissing("spark.app.name", "Landsat Ingest")
  //     .set("spark.default.parallelism", "250000")
  //     .set("spark.serializer", classOf[KryoSerializer].getName)
  //     .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)
  //     .set("spark.network.timeout", "800")


  //   implicit val sc = new SparkContext(conf)

  //   val (writer, updater, as, csvName) =
  //     if(args.contains("local")) {
  //       val writer = FileLayerWriter("/Users/rob/proj/presentations/2017-foss4g-workshop/notebooks/catalog")
  //       val updater = FileLayerUpdater("/Users/rob/proj/presentations/2017-foss4g-workshop/notebooks/catalog")
  //       val as = writer.attributeStore

  //       (writer, updater, as, "l8-scenes-ri.csv")
  //     } else {
  //       val writer = S3LayerWriter("datahub-catalogs-us-east-1", "")
  //       val updater = S3LayerUpdater("datahub-catalogs-us-east-1", "")
  //       val as = writer.attributeStore

  //       (writer, updater, as, "l8-scenes.csv")
  //     }

  //   try {
  //     // Read in csv
  //     val stream: InputStream = getClass.getResourceAsStream(s"/${csvName}")
  //     val scenes =
  //       try {
  //         val full =
  //           scala.io.Source.fromInputStream(stream).getLines
  //             .drop(1)
  //             .map { line =>
  //               val values = line.split(',').map(_.replace("\"", "")).toVector
  //               val time =
  //                 LocalDateTime.parse(values(0), formatter).atOffset(ZoneOffset.UTC).toZonedDateTime

  //               L8Scene(time, values(1), values(2).toInt, values(3))
  //             }
  //             .toList

  //         val filter = { scenes: List[L8Scene] =>
  //           val lf =
  //             if(args.contains("local")) { full.take(5*10) }
  //             else { full }
  //           lf
  //         }

  //         filter(full)
  //       } finally { stream.close() }

  //     println(s"SCENES: ${scenes.size}")

  //     scenes
  //       .groupBy(_.time.
  //       .groupBy(_.time.getDayOfMonth)
  //       .foreach { case (_, dayScenes) =>

  //         val initialPartitions = if(dayScenes.size > 100) { 10000 } else { 50 }
  //         val sceneRdd = sc.parallelize(dayScenes, initialPartitions)

  //       val mapTransform = layoutScheme.levelForZoom(13).layout.mapTransform

  //       val source: RDD[(TemporalProjectedExtent, MultibandTile)] =
  //         sceneRdd
  //           .flatMap { scene =>
  //             val readerOpt =
  //               try {
  //                 Some(StreamingByteReader(S3RangeReader(scene.uri)))
  //               } catch {
  //                 case e: AmazonS3Exception =>
  //                   // Some images are listed but don't exist
  //                   None
  //               }
  //             readerOpt match {
  //               case Some(reader) =>
  //                 val tiffTags = TiffTagsReader.read(reader)
  //                 if(validBounds.contains(tiffTags.extent.reproject(tiffTags.crs, WebMercator))) {
  //                   RasterReader.listWindows(tiffTags.cols, tiffTags.rows, Some(512))
  //                     .map { gridBounds =>
  //                       (scene, gridBounds, (tiffTags.cols, tiffTags.rows))
  //                     }
  //                 } else {
  //                   Array[(L8Scene, GridBounds, (Int, Int))]()
  //                 }
  //               case None => Array[(L8Scene, GridBounds, (Int, Int))]()
  //             }
  //           }
  //           .repartition(numPartitions)
  //           .map { case (scene, gridBounds, (totalCols, totalRows)) =>
  //             // Buffer the pixel window to account for reprojection
  //             val bufferedGridBounds =
  //               GridBounds(
  //                 math.max(0, gridBounds.colMin - 3),
  //                 math.max(0, gridBounds.rowMin - 3),
  //                 math.min(totalCols - 1, gridBounds.colMax + 3),
  //                 math.min(totalRows - 1, gridBounds.rowMax + 3)
  //               )
  //             val reader = StreamingByteReader(S3RangeReader(scene.uri))
  //             val geotiff = SinglebandGeoTiff.streaming(reader)
  //             val raster: Raster[Tile] =
  //               geotiff.raster.crop(bufferedGridBounds).reproject(geotiff.crs, WebMercator, Bilinear)
  //               val projectedExtent = ProjectedExtent(raster.extent, WebMercator)
  //               val tpe = TemporalProjectedExtent(projectedExtent, scene.time)
  //               val tile = raster.tile
  //               ((scene.sceneId, (gridBounds.colMin, gridBounds.rowMin)), (scene.band, tpe, tile))
  //           }
  //           .groupByKey(new HashPartitioner(numPartitions))
  //           .flatMap { case (_, tiles) =>
  //             val tileArr = tiles.toArray
  //             if(tileArr.size != 5) { None }
  //             else {
  //               val (_, tpe, _) = tileArr(0)

  //               val tile =
  //                 MultibandTile(
  //                   tileArr
  //                     .sortBy { case (band, tpe, tile) => band }
  //                     .map(_._3)
  //                 )
  //                 Some((tpe, tile))
  //             }
  //           }

  //       val (zoom, md) = {
  //         val (zoom, md) = source.collectMetadata[SpaceTimeKey](layoutScheme)
  //         println(md.toJson.prettyPrint)
  //         (zoom, md.copy(cellType = UShortConstantNoDataCellType))
  //       }

  //       val tiles = ContextRDD(source.tileToLayout[SpaceTimeKey](md), md)

  //       //      Pyramid.levelStream(tiles, layoutScheme, zoom, 0, Bilinear).foreach { case (z, layer) =>
  //       val (z, layer) = (zoom, tiles)
  //       //{
  //         val lid = LayerId(layerName, z)
  //         if(as.layerExists(lid)) {
  //           // Some manual updating to speed things up
  //           // Taken from S3LayerUpdater
  //           val LayerAttributes(header, metadata, keyIndex, writerSchema) =
  //             as.readLayerAttributes[S3LayerHeader, TileLayerMetadata[SpaceTimeKey], SpaceTimeKey](lid)
  //           val prefix = header.key
  //           val bucket = header.bucket
  //           val maxWidth = Index.digits(keyIndex.toIndex(keyIndex.keyBounds.maxKey))
  //           val keyPath = (key: SpaceTimeKey) => makePath(prefix, Index.encode(keyIndex.toIndex(key), maxWidth))
  //           val updatedMetadata = metadata.merge(layer.metadata)
  //           val codec  = KeyValueRecordCodec[SpaceTimeKey, MultibandTile]
  //           val schema = codec.schema
  //           as.writeLayerAttributes(lid, header, updatedMetadata, keyIndex, schema)
  //           S3RDDWriter.write(layer, bucket, keyPath)
  //         } else {
  //           writer.write(lid, layer, keyIndex)
  //         }
  //         //}
  //     }
  //   } finally {
  //     sc.stop()
  //   }
  // }


  def makePath(chunks: String*) =
    chunks.filter(_.nonEmpty).mkString("/")
}
