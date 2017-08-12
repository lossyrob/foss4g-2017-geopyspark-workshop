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

object FinishPyramid {
  def main(args: Array[String]): Unit = {
    val numPartitions =
      args(0).toInt

    val layerName = "landsat-8-continental-us-2016"
    val layoutScheme = ZoomedLayoutScheme(WebMercator)

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
      .set("spark.default.parallelism", "250000")
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)


    implicit val sc = new SparkContext(conf)

    val (writer, reader, as, csvName) = {
      val writer = S3LayerWriter("datahub-catalogs-us-east-1", "")
      val reader = S3LayerReader("datahub-catalogs-us-east-1", "")
      val as = writer.attributeStore

      (writer, reader, as, "l8-scenes.csv")
    }

    try {

      val tiles = reader.read[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]](
        LayerId(layerName, 13), numPartitions=numPartitions
      )

      Pyramid.levelStream(tiles, layoutScheme, 13, 0, Bilinear).foreach { case (z, layer) =>
        if(z < 13) {
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
        }
      }
    } finally {
      sc.stop()
    }
  }

  def makePath(chunks: String*) =
    chunks.filter(_.nonEmpty).mkString("/")
}
