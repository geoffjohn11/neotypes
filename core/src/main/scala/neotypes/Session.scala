package neotypes

import internal.syntax.async._
import internal.syntax.stage._
import org.neo4j.driver.{TransactionConfig => NeoTransactionConfig}
import org.neo4j.driver.async.{AsyncSession => NeoAsyncSession}
import org.neo4j.driver.internal.shaded.reactor.core.publisher.{Mono}
import org.neo4j.driver.reactive.{RxSession => NeoRxSession}

sealed trait Session[F[_]] {
  def transaction: F[Transaction[F]]

  def transaction(config: NeoTransactionConfig): F[Transaction[F]]

  def transact[T](txF: Transaction[F] => F[T]): F[T]

  def transact[T](config: NeoTransactionConfig)(txF: Transaction[F] => F[T]): F[T]

  def close: F[Unit]
}

object Session {
  private[neotypes] def apply[F[_]](F: Async[F], session: NeoAsyncSession)
                                   (lock: F.Lock): Session[F] = new Session[F] {
    private implicit final val FF: Async[F] = F

    override final def transaction: F[Transaction[F]] =
      transaction(NeoTransactionConfig.empty)

    override final def transaction(config: NeoTransactionConfig): F[Transaction[F]] =
      lock.acquire.flatMap { _ =>
        F.async { cb =>
          session.beginTransactionAsync(config).accept(cb) { tx =>
            Right(Transaction(F, tx)(lock))
          }
        }
      }

    override final def transact[T](txF: Transaction[F] => F[T]): F[T] =
      transact(NeoTransactionConfig.empty)(txF)

    override final def transact[T](config: NeoTransactionConfig)(txF: Transaction[F] => F[T]): F[T] =
      transaction(config).guarantee(txF) {
        case (tx, None)    => tx.commit
        case (tx, Some(_)) => tx.rollback
      }

    override final def close: F[Unit] =
      F.async { cb =>
        session.closeAsync().acceptVoid(cb)
      }
  }
}

sealed trait RxSession[F[_]] {
  def transaction: F[RxTransaction[F]]

  def transaction(config: NeoTransactionConfig): F[RxTransaction[F]]

  def transact[T](txF: RxTransaction[F] => F[T]): F[T]

  def transact[T](config: NeoTransactionConfig)(txF: RxTransaction[F] => F[T]): F[T]

  def close: F[Unit]
}

object RxSession {
  private[neotypes] def apply[F[_]](F: Async[F], session: NeoRxSession)
                                   (lock: F.Lock): RxSession[F] = new RxSession[F] {
    private implicit final val FF: Async[F] = F

    override final def transaction: F[RxTransaction[F]] =
      transaction(NeoTransactionConfig.empty)

    override final def transaction(config: NeoTransactionConfig): F[RxTransaction[F]] =
      lock.acquire.flatMap { _ =>
        F.async { cb =>
          session.beginTransaction().accept(cb){ tx =>
            Right(RxTransaction(F, tx)(lock))
          }
        }
      }

    override final def transact[T](txF: RxTransaction[F] => F[T]): F[T] =
      transact(NeoTransactionConfig.empty)(txF)

    override final def transact[T](config: NeoTransactionConfig)(txF: RxTransaction[F] => F[T]): F[T] =
      transaction(config).guarantee(txF) {
        case (tx, None)    => tx.commit
        case (tx, Some(_)) => tx.rollback
      }

    override final def close: F[Unit] =
      F.async { cb =>
        Mono.from(session.close()).acceptVoid(cb)
      }
  }
}

