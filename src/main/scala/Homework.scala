import java.util.Date
import scala.io.Source
import scala.util.Try

object Homework {
  case class Movie(
                    id: Int,
                    title: String,
                    year: Option[Int],
                    genres: List[String]
                  )

  case class Rating(
                     userId: Int,
                     movieId: Int,
                     rating: Double,
                     ts: Date
                   )

  case class Tag(
                  userId: Int,
                  movieId:Int,
                  tag:String,
                  timestamp:Date
                )

  def parseRating(line: String): Rating = line.split(',').toList match {
    case userId :: movieId :: rating :: ts :: Nil =>
      Rating(
        userId.toInt,
        movieId.toInt,
        rating.toDouble,
        new Date(ts.toLong * 1000)
      )
  }

  def parseTag(line:String) : Tag = line.split(',').toList match {
    case userId :: movieId :: tag :: timestamp :: Nil =>
      Tag(
        userId.toInt,
        movieId.toInt,
        tag,
        new Date(timestamp.toLong * 1000)
      )
  }

  def parseMovie(line: String): Movie = {
    val splitted = line.split(",", 2)

    val id = splitted(0).toInt
    val remaining = splitted(1)
    val sp = remaining.lastIndexOf(",")
    val titleDirty = remaining.substring(0, sp)
    val title =
      if (titleDirty.startsWith("\"")) titleDirty.drop(1).init else titleDirty   // Filmin Adi

    val year = Try(
      title
        .substring(title.lastIndexOf("("), title.lastIndexOf(")"))
        .drop(1)
        .toInt
    ).toOption
    val genres = remaining.substring(sp + 1).split('|').toList
    Movie(id, title, year, genres)
  }

  def findListElementInclusion(firstList: List[Int], secondList: List[Int]): (Int, Int) = {
    var foundElement = 0
    firstList.foreach{i =>
      if (secondList.filter(j => j == i).length >= 1)
        foundElement += 1
    }
    (foundElement,firstList.length)
  }

  def main(args: Array[String]): Unit = {
    // Dataların okunması
    val projectRootPath = new java.io.File(".").getCanonicalPath

    val movieLines =
      Source.fromFile(s"${projectRootPath}/data/movies.csv").getLines().toList.drop(1)
    val ratingLines =
      Source.fromFile(s"${projectRootPath}/data/ratings.csv").getLines().toList.drop(1)
    val tagLines =
      Source.fromFile(s"${projectRootPath}/data/tags.csv").getLines().toList.drop(1)

    // Csvlerin case classlara dönüştürülmesi
    val parsedMovies = movieLines.map(parseMovie)
    val parsedRatings = ratingLines.map(parseRating)
    val parsedTags = tagLines.map(parseTag)

    /*
    * Çözüm 1
    * Yıllara ve türlere (genre) göre film sayıları nedir?
    * Örnek: (1985 korku filmi x adet, aksiyon filmi y adet, 1986 korku filmi x’ adet, aksiyon filmi y’ adet gibi)
    */

    // Tür yıl ve film id'sinin tutulduğu flat edilmiş liste
    val genreYearMovieIdTuple = parsedMovies.map(m => m.genres.map(g => (g,m.year.getOrElse(0),m.id))).flatMap(i => i)

    // Sadece tür ve yıllardan oluşan ve gereksiz dataların filtrelendiği liste
    val genreYearTupleCleansed = genreYearMovieIdTuple
      .map{case (k,v,j) => (k,v)}
      .filter{ case (k,v) => k != "(no genres listed)" &&  k != None && v != 0 }

    println("---------------- Çözüm 1 ---------------- ")
    genreYearTupleCleansed.groupBy(i=>(i._2,i._1))
      .map{case ((k,v),j) => (k,v,j.length)}
      .groupMap(_._1)(m => (m._2,m._3))
      .toList
      .sortBy(_._1)
      .foreach{
        case (k,v) => println(s"------\n${k} yılında:");
          v.foreach{
            case (k,v) => println(s"${v} adet ${k} filmi vardır.")
          }
      }

    /*
    * Çözüm 2
    * Rating puanlarına göre en fazla yüksek puan verilen yıl hangisidir (sinemanın altın yılı 😊)
    */

    // Film id ve rating listesinin olduğu liste
    val movieRatingMap = parsedRatings.map(r => (r.movieId,r.rating)).groupMap(_._1)(_._2)

    // Filmlerin yıllarının ve puanlarının olduğu liste
    val yearRating:List[(Int,List[Double])] = parsedMovies.map(m=>(m.year.getOrElse(0), movieRatingMap.get(m.id).getOrElse(List(0.0d))))

    // Çözüm
    val goldenYearOfCinema = yearRating.groupMap(_._1)(_._2)
      .map{case (k,v) => (k,v.flatMap(i=>i).sum/v.flatMap(i=>i).length.toDouble)}
      .toList
      .sortBy(_._2)
      .reverse
      .head

    println("---------------- Çözüm 2 ---------------- ")
    println(s"Sinemanın altın yılı ${goldenYearOfCinema._2} ortalama puan ile ${goldenYearOfCinema._1} yılıdır.")

    /*
    * Çözüm 3
    * Yıllara göre film adedi en düşük olan türlerin genel ortalamadaki yeri nedir?
    * Örnek:yıllık film adedi en düşük olan 10 yılda toplam 13 filmle romantik komedi türüdür ve toplamda xyz adet film arasında abc adet çekilmiştir
    */

    // Çözüm
    val yearlyLeastProducedCategory =  genreYearTupleCleansed.groupBy(_._1)
      .map{ case(k,v) => (k,v.length,parsedMovies.length) }
      .toList
      .sortBy(_._2)
      .head

    println("---------------- Çözüm 3 ---------------- ")
    println(
      s"Film adedi en düşük olan kategori : ${yearlyLeastProducedCategory._1} türüdür ve" +
        s" toplamda ${yearlyLeastProducedCategory._3} film arasında ${yearlyLeastProducedCategory._2} " +
        s"adet ${yearlyLeastProducedCategory._1} türünde film vardır"
    )

    /*
    * Çözüm 4
    * Türlere göre Tag yapan kullanıcıların rating puanı verme ortalaması nedir ve bu oran hangi yılda peak yapmıştır?
    * Örnek:komedi filmleri için tag veren her 10 kişiden 8’i filme puan da vermektedir ve bu oran 2018 yılında %90’la peak yapmıştır
    */

    // Her bir film id'ye tag veren userid'lerin listesinin tutulduğu liste
    val movieTaggerUserMap = parsedTags.map(t => (t.movieId,t.userId)).groupMap(_._1)(_._2)

    // Her bir film id'ye oy veren userid'lerin listesinin tutulduğu liste
    val movieVoterUserMap = parsedRatings.map(r => (r.movieId,r.userId)).groupMap(_._1)(_._2)

    // Yıl tür ve film id'lerin yanına filme oy ve tag veren user listesinin getirilmesi
    val movieVotersAndTaggers = genreYearMovieIdTuple.map(
      i=>(i._1,i._2,i._3,movieTaggerUserMap.get(i._3).getOrElse(List(0)).toSet.toList,movieVoterUserMap.get(i._3).getOrElse(List(0)))
    ).filter(i => i._3 != List(0) && i._4 != List(0) )

    // Yıl ve tür bazında hem tag hem oy veren user sayısı ve sadece tag veren user sayısının tuple listesi olarak olarak gruplandığı liste
    val movieVotersAndTaggersGrouped = movieVotersAndTaggers.map(m => (m._1,m._2,findListElementInclusion(m._4,m._5)))
      .groupMap(g => (g._1,g._2))(t => (t._3._1,t._3._2))

    // Tupleların toplanarak yıl ve tür bazında hem tag hem oy veren ve sadece tag veren user sayısının tutulduğu liste
    val movieVotersAndTaggersFolded = movieVotersAndTaggersGrouped.map(i => (i._1, i._2.foldLeft((0,0))((r,l) => ((r._1 + l._1,r._2 + l._2)))))

    // Sonuca ulaşmak için outlier'ların elenerek en yüksek oranın bulunması
    val movieVotersAndTaggersSolution = movieVotersAndTaggersFolded.filter(i => i._2._1 != i._2._2)
      .map{case((k,v),(j,m)) => (k,v,j,m,(j.toDouble/m.toDouble)*100)}
      .toList
      .sortBy(_._3)
      .reverse
      .head

    println("---------------- Çözüm 4 ---------------- ")
    println(s"${movieVotersAndTaggersSolution._2} yılında ${movieVotersAndTaggersSolution._1} filmleri için " +
      s"${movieVotersAndTaggersSolution._4} tag veren kişiden ${movieVotersAndTaggersSolution._3}'si" +
      s" filme puan da vermiştir. %${movieVotersAndTaggersSolution._5.toInt} oranla bu yıl peak yapılan yıldır.")

    /*
    * Çözüm 5
    * En fazla tag veren kişinin en sevdiği ve en sevmediği türler hangi yıllardadır?
    * Örnek:519 adet tag’le en fazla tag yapan x id’li kullanıcının en yüksek puan verdiği yıl 1985 yılı aksiyon filmleridir, en az puan verdiği yıl 2000 yılı romantik komedi filmleridir
    */

    // En çok tag veren user
    val taggerKing = parsedTags.groupBy(_.userId)
      .map{ case(k,v) => (k,v.length)}
      .toList
      .sortBy(_._2)
      .reverse
      .head

    // En çok tag veren user'ın film id lere verdiği puanlar
    val ratingsOfTaggerKing = parsedRatings.map(r => (r.userId,r.movieId,r.rating))
      .filter(_._1 == taggerKing._1)
      .map(i => (i._2,i._3))
      .toMap

    // En çok tag veren user'ın yıl ve türlere göre puanlarının gruplanması
    val genreYearAndRatings =  genreYearMovieIdTuple.filter(i=> !ratingsOfTaggerKing.get(i._3).isEmpty )
      .map(i=>(i._1,i._2,ratingsOfTaggerKing.get(i._3).getOrElse(0d)))
      .groupMap(i=>(i._1,i._2))(i => i._3)

    // En çok tag veren user'ın yıl ve türlere göre verdiği oyların ortalamaları(ortalamaya göre artan şekilde sıralanmış)
    val genreYearAvaragesSorted = genreYearAndRatings.map(i => (i._1._1,i._1._2,i._2.sum/i._2.length.toDouble))
      .toList
      .sortBy(_._3)

    // En çok ortalama puana sahip yıl ve tür
    val mostRatedYearAndGenre = genreYearAvaragesSorted.reverse.head

    // En az ortalama puana sahip yıl ve tür
    val leastRatedYearAndGenre = genreYearAvaragesSorted.head

    println("---------------- Çözüm 5 ---------------- ")
    println(s"${taggerKing._2} tag ile ${taggerKing._1} id'li kullanıcının en yüksek puan verdiği yıl" +
      s" ${mostRatedYearAndGenre._2} yılı ${mostRatedYearAndGenre._1} filmleridir," +
      s" en düşük puan verdiği yıl ${leastRatedYearAndGenre._2} yılı ${leastRatedYearAndGenre._1} filmleridir."
    )

    /*
    * Çözüm 6
    * Türlerine göre filmlere önce tag yapılıp sonra mı puan verilmektedir yoksa önce puan verilip sonra mı tag yapılmaktadır?(burada ilk event tag mi yoksa puan mı bakılsa yeterli zira tag-puan-tag şeklinde de gidebilir.)
    */

    // Userların filmlere rating verme timestampleri
    val ratingTimestamps = parsedRatings.map(r => ((r.movieId,r.userId),(r.ts))).toMap

    // Userların filmlere tag yapma timestampleri
    val taggingTimestamps = parsedTags.map(t => ((t.movieId,t.userId),(t.timestamp)))

    // Tagging timestampler ve rating timestamplerin bir araya getirilmesi
    val ratingAndTaggingTimestamps = taggingTimestamps.map(t => (t._1,t._2,ratingTimestamps.get(t._1).getOrElse(t._2)))

    // Önce rating yapılıp sonrasında tagging yapılanların sayısı
    val firstRatingThenTaggingCount = ratingAndTaggingTimestamps.filter(i => i._3.compareTo(i._2) < 0).length

    // Önce tagging yapılıp sonrasında rating yapılanların sayısı
    val firstTaggingThenRatingCount = ratingAndTaggingTimestamps.filter(i => i._3.compareTo(i._2) > 0).length

    // Tagging yapılıp rating yapılmamış olan kayıtlar
    val firstTaggingThenNoRatingCount = ratingAndTaggingTimestamps.filter(i => i._3.compareTo(i._2) == 0).length

    println("---------------- Çözüm 6 ---------------- ")
    println(s"Önce rating yapılıp sonra tagging yapılan kayıt sayısı: ${firstRatingThenTaggingCount}" +
      s"\nÖnce tagging yapılıp sonra rating yapılan kayıt sayısı: ${firstTaggingThenRatingCount}" +
      s"\nTagging yapılıp rating yapılmamıs kayıt sayısı: ${firstTaggingThenNoRatingCount}")
    println("-----------------------------------------")
  }
}