package com.spotify.scio.jupyter

import ammonite.repl.RuntimeAPI
import ammonite.runtime.InterpAPI
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}

/**
 * Allow only one active Scio Context.
 * Also manage the existing Scio Context.
 */
object JupyterScioContextFactory {

  private var _currenntContext: Option[JupyterScioContext] = None
  private var _pipelineOptions: Option[PipelineOptions] = None

  /**
   * Always returns a new Scio Context, and forgets the old context
   */
  def createScioContext(args: (String, String)*)
                       (implicit
                        interpApi: InterpAPI,
                        runtimeApi: RuntimeAPI)
  : JupyterScioContext = {
    _pipelineOptions = Some(
      PipelineOptionsFactory.fromArgs(
        args.map { case (k, v) => s"--$k=$v" }: _*
      ).as(classOf[PipelineOptions])
    )
    _currenntContext = Some(JupyterScioContext(_pipelineOptions.get))
    _currenntContext.get
  }

  /**
   * Get Scio Context with currently defined options.
   * Get new Scio Context if prevous is closed.
   *
   * @return
   */
  def sc: JupyterScioContext = {
    if (_currenntContext.isEmpty || _currenntContext.get.isClosed) {
      // Create a new Scio Context
      _currenntContext = Some(
        JupyterScioContext(_pipelineOptions.getOrElse(PipelineOptionsFactory.create())
        )
      )
    }
    _currenntContext.get
  }

}
