package com.spotify.scio.jupyter

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Path}

import ammonite.repl.RuntimeAPI
import ammonite.runtime.InterpAPI

import com.google.api.services.dataflow.DataflowScopes
import com.google.auth.Credentials
import com.google.auth.oauth2.GoogleCredentials
import com.spotify.scio.{ScioContext, ScioResult}
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

// similar to com.spotify.scio.repl.ReplScioContext

// in the com.spotify.scio namespace to access private[scio] things

class JupyterScioContext(
  options: PipelineOptions,
  replJarPath: Path
)(implicit
  interpApi: InterpAPI,
  runtimeApi: RuntimeAPI
) extends ScioContext(options,
  replJarPath.toAbsolutePath.toString ::
    runtimeApi.sess.frames
      .flatMap(_.classpath)
      .map(_.getAbsolutePath)
) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  interpApi.load.onJarAdded {
    case Seq() => // just in case
    case jars =>
      throw new RuntimeException("Cannot add jars after ScioContext Initialization")
  }

  def setGcpCredentials(credential: Credentials): Unit =
    options.as(classOf[DataflowPipelineOptions]).setGcpCredential(credential)

  def setGcpCredentials(path: String): Unit =
    setGcpCredentials(
      GoogleCredentials.fromStream(new FileInputStream(new File(path))).createScoped(
        List(DataflowScopes.CLOUD_PLATFORM).asJava
      )
    )

  def withDefaultGcpCredentials(): this.type = {
    setGcpCredentials(GoogleCredentials.getApplicationDefault.createScoped(
      List(DataflowScopes.CLOUD_PLATFORM).asJava)
    )
    this
  }

  def withGcpCredentials(credentials: Credentials): this.type = {
    setGcpCredentials(credentials)
    this
  }

  def withGcpCredentials(path: String): this.type = {
    setGcpCredentials(path)
    this
  }

  /** Enhanced version that dumps REPL session jar. */
  override def close(): ScioResult = {
    logger.info("Closing Scio Context") // Some APIs exposed only for Jupyter might close
    runtimeApi.sess.sessionJarFile(replJarPath.toFile)
    super.close()
  }

  private[scio] override def requireNotClosed[T](body: => T) = {
    require(!isClosed, "ScioContext already closed")
    super.requireNotClosed(body)
  }

}

object JupyterScioContext {

  def apply(args: (String, String)*)(implicit
    interpApi: InterpAPI,
    runtimeApi: RuntimeAPI
  ): JupyterScioContext =
    JupyterScioContext(
      PipelineOptionsFactory.fromArgs(
        args
          .map { case (k, v) => s"--$k=$v" }: _*
      ).as(classOf[DataflowPipelineOptions]),
      nextReplJarPath()
    )

  def apply(options: PipelineOptions)(implicit
    interpApi: InterpAPI,
    runtimeApi: RuntimeAPI
  ): JupyterScioContext =
    JupyterScioContext(options, nextReplJarPath())

  def apply(
    options: PipelineOptions,
    replJarPath: Path
  )(implicit
    interpApi: InterpAPI,
    runtimeApi: RuntimeAPI
  ): JupyterScioContext =
    new JupyterScioContext(options, replJarPath)


  def nextReplJarPath(prefix: String = "jupyter-scala-scio-", suffix: String = ".jar"): Path =
    Files.createTempFile(prefix, suffix)

}