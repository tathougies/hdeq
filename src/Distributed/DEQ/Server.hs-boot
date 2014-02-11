module Distributed.DEQ.Server where

    import Distributed.DEQ.Types
    import Distributed.DEQ.Config

    serverStart :: DEQ -> DEQSettings -> IO ()