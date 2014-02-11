{-# LANGUAGE OverloadedStrings #-}
module Main where

import Distributed.DEQ.Config
import Distributed.DEQ.Queue
import Distributed.DEQ.Types

import Control.Monad
import Control.Concurrent

import Data.Configurator
import Data.Configurator.Types
import Data.HashMap.Strict
import Data.String

import System.Environment
import System.Random
import System.Log.Logger

main :: IO ()
main = do
  updateGlobalLogger "Distributed" $ setLevel DEBUG
  [path] <- getArgs
  settings <- loadDEQSettings' [] (fromList [("client.unixSocket", String "deq-socket")])
  deq <- newDEQ settings
  rd <- newQueue deq (fromString path) Global
  case rd of
    Left err -> putStrLn (concat ["Error registering: ", show err])
    Right (wr, rd) -> (putStrLn "Queue created") >>
                      (forever $ do
                          r <- (randomIO :: IO Float)
                          send r wr
                          threadDelay 1000000)