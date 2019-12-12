package neotypes

import internal.syntax.async._
import internal.syntax.stage._

import org.neo4j.driver.v1.{AccessMode, Driver => NDriver}

import scala.jdk.CollectionConverters._
import scala.language.higherKinds

/** neotypes driver responsible for accessing a neo4j graph database
  *
  * @param driver neo4j driver
  * @tparam F effect type for driver
  */
final class Driver[F[_]](private val driver: NDriver) extends AnyVal {

  /** acquire session to database in read mode
    * @note a session is a resource that needs to be allocated and released
    *
    * @param F asynchronous effect type with resource type defined
    * @tparam R effect type that allows creating, using, and releasing a resource
    * @return Session[F] in effect type R
    */
  def session[R[_]](implicit F: Async.Aux[F, R]): R[Session[F]] =
    session[R](accessMode = AccessMode.READ)

  /** acquire session to database in read or write mode
    * @note a session is a resource that needs to be allocated and released
    *
    * @param accessMode read or write mode
    * @param bookmarks bookmarks passed between transactions for neo4j casual chaining
    * @param F asynchronous effect type with resource type defined
    * @tparam R effect type that allows creating, using, and releasing a resource
    * @return Session[F] in effect type R
    */
  def session[R[_]](accessMode: AccessMode, bookmarks: String*)
                   (implicit F: Async.Aux[F, R]): R[Session[F]] =
    F.resource(createSession(accessMode, bookmarks))(session => session.close)

  /** create a session from the neo4j driver
    *
    * @param accessMode read or write mode
    * @param bookmarks bookmarks passed between transactions for neo4j casual chaining
    * @return Session of effect type F
    */
  private[this] def createSession(accessMode: AccessMode, bookmarks: Seq[String] = Seq.empty): Session[F] =
    new Session(
      bookmarks match {
        case Seq()         => driver.session(accessMode)
        case Seq(bookmark) => driver.session(accessMode, bookmark)
        case _             => driver.session(accessMode, bookmarks.asJava)
      }
    )

  /** apply a unit of work to a read session
    *
    * @param sessionWork function that takes a Session[F] and returns an F[T]
    * @param F the effect type
    * @tparam T the type of the value that will be returned when the query is executed.
    * @return an effect F of type T
    */
  def readSession[T](sessionWork: Session[F] => F[T])
                    (implicit F: Async[F]): F[T] =
    withSession(AccessMode.READ)(sessionWork)

  /** apply a unit to a write session
    *
    * @param sessionWork function that takes a Session[F] and returns an F[T]
    * @param F the effect type
    * @tparam T the type of the value that will be returned when the query is executed.
    * @return an effect F of type T
    */
  def writeSession[T](sessionWork: Session[F] => F[T])
                     (implicit F: Async[F]): F[T] =
    withSession(AccessMode.WRITE)(sessionWork)

  private[this] def withSession[T](accessMode: AccessMode)
                                  (sessionWork: Session[F] => F[T])
                                  (implicit F: Async[F]): F[T] =
    F.delay(createSession(accessMode)).guarantee(sessionWork) {
      case (session, _) => session.close
    }

  /** close the resources assigned to the neo4j driver
    *
    * @param F the effect type
    * @return an effect F of Unit
    */
  def close(implicit F: Async[F]): F[Unit] =
    F.async { cb =>
      driver.closeAsync().acceptVoid(cb)
    }
}
