{-# LANGUAGE OverloadedStrings #-}

import           Control.Concurrent           (threadDelay)
import           Control.Monad.IO.Class       (MonadIO, liftIO)
import           Control.Monad.Trans.Resource (runResourceT)
import qualified Data.ByteString.Lazy.Char8   as BL
import           Data.Conduit
import           Network.AMQP
import           Network.AMQP.Conduit
import           Test.Hspec

main :: IO ()
main = hspec $ do
    describe "produce and consume test" $ do
        it "send a message and recieve the message" $ do
            runResourceT $ withChannel config $ \conn -> do
                sendMsg str $$ amqpSendSink conn "myExchange" "myKey"
            amqp <- createConnectionChannel config
            amqp' <- createConsumer amqp "myQueue" Ack $ \(msg,env) -> do
                amqpReceiveSource (msg,env) $$ printMsg
            -- | NOTE: RabbitMQ 1.7 doesn't implement this command.
            -- amqp'' <- pauseConsumers amqp'
            -- amqp''' <- resumeConsumers amqp''
            threadDelay $ 15  * 1000000
            _ <- deleteConsumer amqp'
            return ()

str :: String
str = "This is a test message"

config :: AmqpConf
config = AmqpConf "amqp://guest:guest@192.168.59.103:5672/" queue exchange "myKey"
    where
        exchange = newExchange {exchangeName = "myExchange", exchangeType = "direct"}
        queue = newQueue {queueName = "myQueue"}

sendMsg :: (Monad m, MonadIO m) => String -> Source m Message
sendMsg msg = do
    yield (newMsg {msgBody = (BL.pack msg),msgDeliveryMode = Just Persistent} )

printMsg :: (Monad m, MonadIO m) => Sink (Message, Envelope) m ()
printMsg = do
    m <- await
    case m of
       Nothing -> printMsg
       Just (msg,env) -> do
           liftIO $ ackEnv env
           liftIO $ (BL.unpack $ msgBody msg) `shouldBe` str
           liftIO $ putStrLn $ "received message: " ++ (BL.unpack $ msgBody msg)
           -- printMsg
