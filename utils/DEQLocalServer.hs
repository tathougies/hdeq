{-# LANGUAGE OverloadedStrings #-}
module Main where

import Distributed.DEQ.Config
import Distributed.DEQ.Queue
import Distributed.DEQ.Types

import Data.Configurator
import Data.Configurator.Types
import Data.HashMap.Strict

import System.Log.Logger

main :: IO ()
main = do
  updateGlobalLogger "Distributed" $ setLevel DEBUG
  settings <- loadDEQSettings' [] (fromList [("host.disabled", Bool False),
                                             ("host.unixSocket", String "deq-socket"),
                                             ("client.disabled", Bool True)])
  deq <- newDEQ settings
  infoM "Distributed" (concat ["root server: ", show (isRootServer deq)])
  serveDEQ deq