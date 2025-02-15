# Copyright 2012 Jose Blanca, Peio Ziarsolo, COMAV-Univ. Politecnica Valencia
# This file is part of ngs_crumbs.
# ngs_crumbs is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.

# ngs_crumbs is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR  PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with ngs_crumbs. If not, see <http://www.gnu.org/licenses/>.

from __future__ import division
from collections import Counter, namedtuple

from crumbs.vcf.statistics import choose_samples
from crumbs.iterutils import RandomAccessIterator
from crumbs.utils.optional_modules import scipy_fisher


# Missing docstring
# pylint: disable=C0111

DEF_SNV_WIN = 101
DEF_R_SQR_THRESHOLD = 0.01
DEF_P_VAL = 0.01
MIN_PHYS_DIST = 700  # it should be double of the read length

HaploCount = namedtuple('HaploCount', ['AB', 'Ab', 'aB', 'ab'])
Alleles = namedtuple('Alleles', ['A', 'B', 'a', 'b'])
LDStats = namedtuple('LDStats', ['fisher', 'r_sqr'])


def calculate_ld_stats(snp1, snp2, samples=None):
    calls1 = choose_samples(snp1.record, sample_names=samples)
    calls2 = choose_samples(snp2.record, sample_names=samples)
    haplo_counts = _count_biallelic_haplotypes(calls1, calls2)
    fisher = _fisher_exact(haplo_counts)
    rsqr = _calculate_r_sqr(haplo_counts)
    return LDStats(fisher, rsqr)


def _fisher_exact(haplo_counts):
    if not haplo_counts:
        return None
    fish = scipy_fisher(([haplo_counts.AB, haplo_counts.Ab],
                         [haplo_counts.aB, haplo_counts.ab]))[1]
    return fish


def fisher_exact(snp1, snp2, samples=None):
    calls1 = choose_samples(snp1.record, sample_names=samples)
    calls2 = choose_samples(snp2.record, sample_names=samples)
    haplo_counts = _count_biallelic_haplotypes(calls1, calls2)
    return _fisher_exact(haplo_counts)


def calculate_r_sqr(snp1, snp2, samples=None):
    calls1 = choose_samples(snp1.record, sample_names=samples)
    calls2 = choose_samples(snp2.record, sample_names=samples)
    haplo_counts = _count_biallelic_haplotypes(calls1, calls2)

    if haplo_counts is None:
        return None
    return _calculate_r_sqr(haplo_counts)


def _calculate_r_sqr(haplo_counts):
    # Invalid name. Due to using uppercases
    # pylint: disable=C0103
    if haplo_counts is None:
        return None
    total_samples = sum(haplo_counts)
    cnt_AB = haplo_counts.AB
    cnt_Ab = haplo_counts.Ab
    cnt_aB = haplo_counts.aB
    cnt_ab = haplo_counts.ab

    freqA = (cnt_AB + cnt_Ab) / total_samples
    freqB = (cnt_AB + cnt_aB) / total_samples

    if freqA == 0 or freqB == 0 or freqA == 1 or freqB == 1:
        return None

    rsqr = ((cnt_AB / total_samples) * (cnt_ab / total_samples))
    rsqr -= ((cnt_aB / total_samples) * (cnt_Ab / total_samples))
    rsqr *= rsqr
    rsqr /= ((freqA * (1 - freqA)) * (freqB * (1 - freqB)))
    return rsqr


def _most_freq_alleles(calls):
    gts_counts = Counter()
    calls = list(calls)
    if calls:
        if hasattr(calls[0], 'depth'):
            alleles = [al for call in calls for al in call.call.gt_alleles]
        else:
            alleles = [al for call in calls for al in call.gt_alleles]
    else:
        alleles = []
    gts_counts.update(alleles)
    return [al for al, count in gts_counts.most_common(2)]


def _remove_no_calls_and_hets(calls1, calls2):
    filtered_calls1 = []
    filtered_calls2 = []
    for call1, call2 in zip(calls1, calls2):
        if (not call1.called or not call2.called or call1.is_het or
                call2.is_het):
            continue
        filtered_calls1.append(call1)
        filtered_calls2.append(call2)
    return filtered_calls1, filtered_calls2


def _count_biallelic_haplotypes(calls1, calls2, return_alleles=False):
    # Invalid name. Due to using uppercases
    # pylint: disable=C0103
    calls1, calls2 = _remove_no_calls_and_hets(calls1, calls2)
    freq_alleles = []
    freq_alleles.append(_most_freq_alleles(calls1))
    freq_alleles.append(_most_freq_alleles(calls2))

    haplo_count = Counter()
    pyvcf = None
    for call1, call2 in zip(calls1, calls2):
        if pyvcf is None:
            pyvcf = False if hasattr(call1, 'depth') else True
        if pyvcf:
            al_snp_1 = call1.gt_alleles[0]
        else:
            al_snp_1 = call1.call.gt_alleles[0]
        # We're transforming all markers in biallelic
        if al_snp_1 != freq_alleles[0][0]:
            al_snp_1 = freq_alleles[0][1]
        if pyvcf:
            al_snp_2 = call2.gt_alleles[0]
        else:
            al_snp_2 = call2.call.gt_alleles[0]
        if al_snp_2 != freq_alleles[1][0]:
            al_snp_2 = freq_alleles[1][1]

        haplo = (al_snp_1, al_snp_2)
        haplo_count[haplo] += 1
    if not haplo_count:
        return None

    alleles_snp1, alleles_snp2 = zip(*[(haplo[0], haplo[1])
                                       for haplo in haplo_count.keys()])
    alleles_snp1 = set(alleles_snp1)
    alleles_snp2 = set(alleles_snp2)

    haplo_AB = haplo_count.most_common(1)[0][0]
    allele_A = haplo_AB[0]
    allele_B = haplo_AB[1]

    non_A_alleles = list(alleles_snp1.difference(allele_A))
    non_B_alleles = list(alleles_snp2.difference(allele_B))

    if not non_A_alleles or not non_B_alleles:
        return None
    allele_a = non_A_alleles[0]
    allele_b = non_B_alleles[0]

    count_AB = haplo_count.get(haplo_AB, 0)
    count_Ab = haplo_count.get((allele_A, allele_b), 0)
    count_aB = haplo_count.get((allele_a, allele_B), 0)
    count_ab = haplo_count.get((allele_a, allele_b), 0)

    counts = HaploCount(count_AB, count_Ab, count_aB, count_ab)
    if return_alleles:
        alleles = Alleles(allele_A, allele_B, allele_a, allele_b)
        return counts, alleles
    else:
        return counts


def calc_recomb_rate(calls1, calls2, pop_type):
    haplo_count = _count_biallelic_haplotypes(calls1, calls2)
    if haplo_count is None:
        return None

    recomb_haplos = haplo_count.aB + haplo_count.Ab
    tot_haplos = sum(haplo_count)

    if pop_type == 'ril_self':
        recomb = recomb_haplos * (tot_haplos - recomb_haplos - 1)
        recomb /= 2 * (tot_haplos - recomb_haplos) ** 2
    elif pop_type in ('test_cross', 'dihaploid'):
        recomb = recomb_haplos / tot_haplos
    else:
        msg = 'recomb. rate calculation not implemented for pop_type: %s'
        msg %= pop_type
        raise NotImplementedError(msg)
    return recomb, haplo_count


class _LDStatsCache(object):
    def __init__(self):
        self.cache = {}

    @staticmethod
    def _sort_index(snv1, snv2):
        return tuple(sorted((snv1, snv2)))

    def set_stat(self, snv1, snv2, value):
        index = self._sort_index(snv1, snv2)
        try:
            cache_indx1 = self.cache[index[0]]
        except KeyError:
            cache_indx1 = {}
            self.cache[index[0]] = cache_indx1
        cache_indx1[index[1]] = value

    def get_stat(self, snv1, snv2):
        index = self._sort_index(snv1, snv2)
        cache_indx1 = self.cache[index[0]]
        return cache_indx1[index[1]]

    def del_lower_than(self, min_index0):
        cache = self.cache
        to_del = []
        for key in cache.keys():
            if key < min_index0:
                to_del.append(key)
        for key in to_del:
            del cache[key]


def filter_snvs_by_ld(snvs, samples=None, r_sqr=DEF_R_SQR_THRESHOLD,
                      p_val=DEF_P_VAL, bonferroni=True, snv_win=DEF_SNV_WIN,
                      min_phys_dist=MIN_PHYS_DIST, log_fhand=None):
    if not snv_win % 2:
        msg = 'The window should have an odd number of snvs'
        raise ValueError(msg)
    half_win = (snv_win - 1) // 2

    if bonferroni:
        p_val /= (snv_win - 1)

    snvs = RandomAccessIterator(snvs, rnd_access_win=snv_win)
    linked_snvs = set()
    total_snvs = 0
    passed_snvs = 0
    prev_chrom = None
    stats_cache = _LDStatsCache()
    for snv_i, snv in enumerate(snvs):
        total_snvs += 1
        if snv_i in linked_snvs:
            yield snv
            passed_snvs += 1
            linked_snvs.remove(snv_i)
            continue
        linked = None
        win_start = snv_i - half_win

        this_chrom = snv.chrom
        if prev_chrom is None:
            prev_chrom = this_chrom
        if prev_chrom != this_chrom:
            stats_cache = _LDStatsCache()

        if win_start < 0:
            win_start = 0
        for snv_j in range(snv_i + half_win, win_start - 1, -1):
            try:
                snv_2 = snvs[snv_j]
            except IndexError:
                continue

            if snv_i == snv_j:
                continue

            try:
                linked = stats_cache.get_stat(snv_i, snv_j)
                in_cache = True
            except KeyError:
                in_cache = False

            if in_cache:
                pass
            elif snv.chrom != snv_2.chrom:
                # different chroms, they're not linked
                linked = False
            elif abs(snv.pos - snv_2.pos) < min_phys_dist:
                # Too close, they could be errors due to the same reads
                # so no independent errors
                linked = None
            else:
                stats = calculate_ld_stats(snv, snv_2, samples=samples)
                if stats.r_sqr >= r_sqr and stats.fisher < p_val:
                    linked = True
                    if snv_j > snv_i:
                        linked_snvs.add(snv_j)
                    break
                else:
                    linked = False
            if not linked:
                stats_cache.set_stat(snv_i, snv_j, linked)

        if linked:
            yield snv
            passed_snvs += 1
        stats_cache.del_lower_than(win_start)

    if log_fhand is not None:
        _write_log(log_fhand, total_snvs, passed_snvs)


def _write_log(log_fhand, total_snvs, passed_snvs):
    log_fhand.write('SNVs processed: ' + str(total_snvs) + '\n')
    msg = 'SNVs passsed: ' + str(passed_snvs) + '\n'
    log_fhand.write(msg)
    msg = 'SNVs filtered out: ' + str(total_snvs - passed_snvs) + '\n'
    log_fhand.write(msg)
