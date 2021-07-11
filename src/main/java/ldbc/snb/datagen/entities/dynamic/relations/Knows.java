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
package ldbc.snb.datagen.entities.dynamic.relations;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.entities.dynamic.person.PersonSummary;
import ldbc.snb.datagen.util.RNG;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;


public final class Knows implements Writable, Comparable<Knows>, Serializable {

    private boolean isExplicitlyDeleted;
    private PersonSummary to;
    private long creationDate;
    private long deletionDate;
    private float weight = 0.0f;
    public static int num = 0;

    public Knows() {
        to = new PersonSummary();
    }

    public Knows(Knows k) {
        to = new PersonSummary(k.to());
        creationDate = k.getCreationDate();
        deletionDate = k.getDeletionDate();
        weight = k.getWeight();
        isExplicitlyDeleted = k.isExplicitlyDeleted();
    }

    public Knows(Person to, long creationDate, long deletionDate, float weight, boolean isExplicitlyDeleted) {
        this.to = new PersonSummary(to);
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.weight = weight;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public boolean isExplicitlyDeleted() {
        return isExplicitlyDeleted;
    }

    public void setExplicitlyDeleted(boolean explicitlyDeleted) {
        isExplicitlyDeleted = explicitlyDeleted;
    }

    public PersonSummary to() {
        return to;
    }

    public void to(PersonSummary to) {
        this.to.copy(to);
    }

    public PersonSummary getTo() {
        return to;
    }

    public void setTo(PersonSummary to) {
        this.to = to;
    }

    public long getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(long creationDate) {
        this.creationDate = creationDate;
    }

    public long getDeletionDate() {
        return deletionDate;
    }

    public void setDeletionDate(long deletionDate) {
        this.deletionDate = deletionDate;
    }

    public void setWeight(float weight) {
        this.weight = weight;
    }

    public float getWeight() {
        return weight;
    }

    public void readFields(DataInput arg0) throws IOException {
        isExplicitlyDeleted = arg0.readBoolean();
        to.readFields(arg0);
        creationDate = arg0.readLong();
        deletionDate = arg0.readLong();
        weight = arg0.readFloat();
    }

    public void write(DataOutput arg0) throws IOException {
        arg0.writeBoolean(isExplicitlyDeleted);
        to.write(arg0);
        arg0.writeLong(creationDate);
        arg0.writeLong(deletionDate);
        arg0.writeFloat(weight);
    }

    public int compareTo(Knows k) {
        long res = (to.getAccountId() - k.to().getAccountId());
        if (res > 0) return 1;
        if (res < 0) return -1;
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Knows knows = (Knows) o;
        return isExplicitlyDeleted == knows.isExplicitlyDeleted &&
                creationDate == knows.creationDate &&
                deletionDate == knows.deletionDate &&
                Float.compare(knows.weight, weight) == 0 &&
                Objects.equals(to, knows.to);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isExplicitlyDeleted, to, creationDate, deletionDate, weight);
    }

    static public class FullComparator implements Comparator<Knows> {

        public int compare(Knows a, Knows b) {
            long res = (a.to.getAccountId() - b.to().getAccountId());
            if (res > 0) return 1;
            if (res < 0) return -1;
            long res2 = a.creationDate - b.getCreationDate();
            if (res2 > 0) return 1;
            if (res2 < 0) return -1;
            return 0;
        }

    }

    public static boolean createKnow(RNG dateRandom, RNG deletionRandom, Person personA, Person personB, Person.PersonSimilarity personSimilarity, Boolean ignore) {

        if (personA.getCreationDate() + DatagenParams.delta > personB.getDeletionDate() ||
                personB.getCreationDate() + DatagenParams.delta > personA.getDeletionDate()) {
            return false;
        }

        long creationDate = Dictionaries.dates.randomKnowsCreationDate(dateRandom, personA, personB);
        long deletionDate;
        boolean isExplicitlyDeleted;

        float similarity = personSimilarity.similarity(personA, personB);

        double deleteProb;
        if (similarity < 0.9222521) {
            deleteProb = 0.025;
        } else {
            deleteProb = 0.075;
        }

        if (deletionRandom.nextDouble() < deleteProb) {
            isExplicitlyDeleted = true;
            deletionDate = Dictionaries.dates.randomKnowsDeletionDate(dateRandom, personA, personB, creationDate);
        } else {
            isExplicitlyDeleted = false;
            deletionDate = Collections.min(Arrays.asList(personA.getDeletionDate(), personB.getDeletionDate()));
        }
        assert (creationDate <= deletionDate) : "Knows creation date is larger than knows deletion date";

        return personB.getKnows().add(new Knows(personA, creationDate, deletionDate, similarity,isExplicitlyDeleted)) &&
                personA.getKnows().add(new Knows(personB, creationDate, deletionDate, similarity,isExplicitlyDeleted));
    }

    //     TODO: used for uni and interest dimension in knows gen
    public static void createKnow(RNG dateRandom, RNG deletionRandom, Person personA, Person personB, Person.PersonSimilarity personSimilarity) {

        if (personA.getCreationDate() + DatagenParams.delta > personB.getDeletionDate() ||
                personB.getCreationDate() + DatagenParams.delta > personA.getDeletionDate()) {
            return;
        }

        float similarity = personSimilarity.similarity(personA, personB);

        long creationDate = Dictionaries.dates.randomKnowsCreationDate(dateRandom, personA, personB);
        long deletionDate;
        boolean isExplicitlyDeleted;
        double deleteProb;
        if (similarity < 0.9222521) {
            deleteProb = 0.025;
        } else {
            deleteProb = 0.075;
        }

        if (deletionRandom.nextDouble() < deleteProb) {
            isExplicitlyDeleted = true;
            deletionDate = Dictionaries.dates.randomKnowsDeletionDate(dateRandom, personA, personB, creationDate);
        } else {
            isExplicitlyDeleted = false;
            deletionDate = Collections.min(Arrays.asList(personA.getDeletionDate(), personB.getDeletionDate()));
        }

        assert (creationDate <= deletionDate) : "Knows creation date is larger than knows deletion date";

        if (personB.getKnows().add(new Knows(personA, creationDate, deletionDate, similarity, isExplicitlyDeleted))) {
            personA.getKnows().add(new Knows(personB, creationDate, deletionDate, similarity, isExplicitlyDeleted));
        }
    }

    public static long targetEdges(Person person, List<Float> percentages, int step_index) {
        int generated_edges = 0;
        for (int i = 0; i < step_index; ++i) {
            generated_edges += Math.ceil(percentages.get(i) * person.getMaxNumKnows());
        }
        generated_edges = Math.min(generated_edges, (int) person.getMaxNumKnows());
        return Math.min((int) person.getMaxNumKnows() - generated_edges, (int) Math
                .ceil(percentages.get(step_index) * person.getMaxNumKnows()));
    }
}
