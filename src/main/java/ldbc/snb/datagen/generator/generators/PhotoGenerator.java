/* 
 Copyright (c) 2013 LDBC
 Linked Data Benchmark Council (http://www.ldbcouncil.org)
 
 This file is part of ldbc_snb_datagen.
 
 ldbc_snb_datagen is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 
 ldbc_snb_datagen is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with ldbc_snb_datagen.  If not, see <http://www.gnu.org/licenses/>.
 
 Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
 All Rights Reserved.
 
 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation;  only Version 2 of the License dated
 June 1991.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.*/
package ldbc.snb.datagen.generator.generators;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.messages.Photo;
import ldbc.snb.datagen.entities.dynamic.person.IP;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import ldbc.snb.datagen.util.*;
import ldbc.snb.datagen.vocabulary.SN;
import org.javatuples.Pair;

import java.util.*;
import java.util.stream.Stream;

/**
 * This class generates photos which are used in posts.
 */
class PhotoGenerator {

    private LikeGenerator likeGenerator;
    private Photo photo;

    PhotoGenerator(LikeGenerator likeGenerator) {
        this.likeGenerator = likeGenerator;
        this.photo = new Photo();
    }

    Stream<Pair<Photo, Stream<Like>>> createPhotos(RandomGeneratorFarm randomFarm, final Forum album, long numPhotosInAlbum, Iterator<Long> idIterator, long blockId) {
        int numPopularPlaces = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_POPULAR)
                .nextInt(DatagenParams.maxNumPopularPlaces + 1);
        List<Short> popularPlaces = new ArrayList<>();
        for (int i = 0; i < numPopularPlaces; i++) {
            short aux = Dictionaries.popularPlaces.getPopularPlace(randomFarm
                    .get(RandomGeneratorFarm.Aspect.POPULAR), album
                    .getPlaceId());
            if (aux != -1) {
                popularPlaces.add(aux);
            }
        }

        return Streams.stream(Iterators.forIterator(0, i -> i < numPhotosInAlbum, i -> ++i, i -> {
            TreeSet<Integer> tags = new TreeSet<>();

            // creates photo
            long creationDate = album.getCreationDate() + DatagenParams.delta + 1000 * (i + 1);
            if (creationDate >= album.getDeletionDate()) {
                return Iterators.ForIterator.BREAK();
            }

            RNG randomDate = randomFarm.get(RandomGeneratorFarm.Aspect.DATE);
            RNG randomDeletePost = randomFarm.get(RandomGeneratorFarm.Aspect.DELETION_POST);

            long deletionDate;
            boolean isExplicitlyDeleted;
            if (album.getModerator().isMessageDeleter() && randomDeletePost.nextDouble() < DatagenParams.probPhotoDeleted) {
                isExplicitlyDeleted = true;
                long minDeletionDate = creationDate + DatagenParams.delta;
                long maxDeletionDate = Math.min(album.getDeletionDate(), Dictionaries.dates.getSimulationEnd());
                deletionDate = Dictionaries.dates.powerLawDeleteDate(randomDate, minDeletionDate, maxDeletionDate);
            } else {
                isExplicitlyDeleted = false;
                deletionDate = album.getDeletionDate();
            }

            int country = album.getModerator().getCountry();
            IP ip = album.getModerator().getIpAddress();
            RNG random = randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP_FOR_TRAVELER);


            if (PersonBehavior.changeUsualCountry(random, creationDate)) {
                random = randomFarm.get(RandomGeneratorFarm.Aspect.COUNTRY);
                country = Dictionaries.places.getRandomCountryUniform(random);
                random = randomFarm.get(RandomGeneratorFarm.Aspect.IP);
                ip = Dictionaries.ips.getIP(random, country);
            }

            long id = SN.formId(SN.composeId(idIterator.next(), creationDate), blockId);
            photo.initialize(id,
                    creationDate,
                    deletionDate,
                    album.getModerator(),
                    album.getId(),
                    "photo" + id + ".jpg",
                    new ArrayList<>(tags),
                    country,
                    ip,
                    album.getModerator().getBrowserId(),
                    isExplicitlyDeleted
            );

            Stream<Like> likeStream = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE).nextDouble() <= 0.1
                    ? likeGenerator.generateLikes(
                            randomFarm.get(RandomGeneratorFarm.Aspect.DELETION_LIKES),
                            randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE),
                            album, photo, Like.LikeType.PHOTO)
                    : Stream.empty();

            return Iterators.ForIterator.RETURN(new Pair<>(photo, likeStream)); // (photo, likeStream)
        }));
    }

}
