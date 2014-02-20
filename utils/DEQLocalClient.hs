{-# LANGUAGE OverloadedStrings #-}
module Main where

import Distributed.DEQ.Config
import Distributed.DEQ.Queue

import Control.Monad

import Data.Configurator
import Data.Configurator.Types
import Data.HashMap.Strict
import Data.String

import System.Environment
import System.Log.Logger

main :: IO ()
main = do
  updateGlobalLogger "Distributed" $ setLevel DEBUG
  [path] <- getArgs
  settings <- loadDEQSettings ProducerConsumer
  deq <- newDEQ settings
  rd <- joinQueue deq (fromString path)
  case rd of
    Left err -> putStrLn (concat ["Error joining: ", show err])
    Right rd -> forever $ do
      x <- receive rd
      case x of
        Left err -> putStrLn (concat ["Error: ", show err])
        Right x -> putStrLn (concat ["Received: ", show (x :: Float)])