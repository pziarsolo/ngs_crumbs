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

import os.path
from subprocess import check_call, CalledProcessError
import shutil
from tempfile import NamedTemporaryFile
import sys
from array import array

from crumbs.bam.flag import create_flag
from crumbs.settings import get_setting
from crumbs.utils.bin_utils import get_num_threads
from crumbs.utils.optional_modules import (AlignmentFile, view, index, faidx,
                                           calmd)
LEFT_DOWNGRADED_TAG = 'dl'
RIGTH_DOWNGRADED_TAG = 'dr'
# pylint: disable=C0111


def filter_bam(in_fpath, out_fpath, min_mapq=0, required_flag_tags=None,
               filtering_flag_tags=None, regions=None):
    cmd = ['-bh']

    # The following line:
    cmd.append('-o' + out_fpath)
    # should be
    # cmd.extend(['-o', out_fpath])
    # but it is a workaround, take a look at:
    # https://groups.google.com/forum/#!msg/pysam-user-group/ooHgIiNVe4c/CcY06d45rzQJ

    if min_mapq:
        cmd.extend(['-q', str(min_mapq)])

    if required_flag_tags:
        flag = create_flag(required_flag_tags)
        cmd.extend(['-f', str(flag)])

    if filtering_flag_tags:
        flag = create_flag(filtering_flag_tags)
        cmd.extend(['-F', str(flag)])

    cmd.extend([in_fpath])

    if regions:
        regions = ['{0}:{1}-{2}'.format(*s) for s in regions.segments]
        cmd.extend(regions)

    view(*cmd)


def sort_bam(in_bam_fpath, out_bam_fpath=None):

    if out_bam_fpath is None:
        out_bam_fpath = in_bam_fpath

    if out_bam_fpath == in_bam_fpath:
        sorted_fhand = NamedTemporaryFile(suffix='.sorted.bam', delete=False)
        temp_out_fpath = sorted_fhand.name
    else:
        temp_out_fpath = out_bam_fpath

    picard_jar = get_setting("PICARD_JAR")
    cmd = ['java', '-jar', picard_jar, 'SortSam',
           'INPUT={0}'.format(in_bam_fpath),
           'OUTPUT={0}'.format(temp_out_fpath),
           'SORT_ORDER=coordinate', 'VALIDATION_STRINGENCY=LENIENT']
    stderr = NamedTemporaryFile(suffix='picard.stderr')
    check_call(cmd, stderr=stderr)

    if temp_out_fpath != out_bam_fpath:
        shutil.move(temp_out_fpath, out_bam_fpath)


def index_bam(bam_fpath):
    'It indexes a bam file'
    index(bam_fpath)


def _create_sam_reference_index(fpath):
    'It creates a sam index for a reference sequence file'
    index_fpath = fpath + '.fai'
    if os.path.exists(index_fpath):
        return
    faidx(fpath)


def _create_picard_dict(fpath):
    'It creates a picard dict if if it does not exist'
    dict_path = os.path.splitext(fpath)[0] + '.dict'
    if os.path.exists(dict_path):
        return
    picard_jar = get_setting("PICARD_JAR")
    cmd = ['java', '-jar', picard_jar, 'CreateSequenceDictionary',
           'R=%s' % fpath,
           'O=%s' % dict_path]
    stderr = NamedTemporaryFile(suffix='picard.stderr')
    check_call(cmd, stderr=stderr)


def realign_bam(in_bam_fpath, reference_fpath, out_bam_fpath=None):

    if out_bam_fpath is None:
        out_bam_fpath = in_bam_fpath

    if out_bam_fpath == in_bam_fpath:
        realigned_fhand = NamedTemporaryFile(suffix='.realigned.bam',
                                             delete=False)
        temp_out_fpath = realigned_fhand.name
    else:
        temp_out_fpath = out_bam_fpath

    _realign_bam(in_bam_fpath, reference_fpath, temp_out_fpath, threads=False)
    sort_bam(temp_out_fpath)

    if temp_out_fpath != out_bam_fpath:
        shutil.move(temp_out_fpath, out_bam_fpath)


def _realign_bam(bam_fpath, reference_fpath, out_bam_fpath, threads=False):
    'It realigns the bam using GATK Local realignment around indels'
    # reference sam index
    _create_sam_reference_index(reference_fpath)

    # reference picard dict
    _create_picard_dict(reference_fpath)

    # bam index
    index_bam(bam_fpath)

    # the intervals to realign
#     gatk_dir = get_setting("GATK_DIR")
#     gatk_jar = os.path.join(gatk_dir, 'GenomeAnalysisTK.jar')
    gatk_jar = get_setting('GATK_JAR')
    intervals_fhand = NamedTemporaryFile(suffix='.intervals')
    stderr = NamedTemporaryFile(suffix='picard.stderr')
    stdout = NamedTemporaryFile(suffix='picard.stdout')
    cmd = ['java', '-jar', gatk_jar, '-T', 'RealignerTargetCreator',
           '-I', bam_fpath, '-R', reference_fpath, '-o', intervals_fhand.name]
    check_call(cmd, stderr=stderr, stdout=stdout)

    # the realignment itself
    cmd = ['java', '-jar', gatk_jar, '-I', bam_fpath, '-R', reference_fpath,
           '-T', 'IndelRealigner', '-targetIntervals', intervals_fhand.name,
           '-o', out_bam_fpath]

    if threads and threads > 1:
        cmd.extend(['-nt', str(get_num_threads(threads))])
    check_call(cmd, stderr=stderr, stdout=stdout)
    intervals_fhand.close()


def calmd_bam(in_bam_fpath, reference_fpath, out_bam_fpath=None):

    if out_bam_fpath is None:
        out_bam_fpath = in_bam_fpath

    if out_bam_fpath == in_bam_fpath:
        realigned_fhand = NamedTemporaryFile(suffix='.realigned.bam',
                                             delete=False)
        temp_out_fpath = realigned_fhand.name
    else:
        temp_out_fpath = out_bam_fpath

    _calmd_bam(in_bam_fpath, reference_fpath, temp_out_fpath)

    if temp_out_fpath != out_bam_fpath:
        shutil.move(temp_out_fpath, out_bam_fpath)


def _calmd_bam(bam_fpath, reference_fpath, out_bam_fpath):
    out_fhand = open(out_bam_fpath, 'wb')
    for line in calmd(*["-bAr", bam_fpath, reference_fpath]):
        out_fhand.write(line)
    # out_fhand.write(pysam.calmd(*["-bAr", bam_fpath, reference_fpath]))
    out_fhand.flush()
    out_fhand.close()


def merge_sams(in_fpaths, out_fpath):
    picard_jar = get_setting("PICARD_JAR")

    cmd = ['java', '-jar', picard_jar, 'MergeSamFiles',
           'O={}'.format(out_fpath)]
    for in_fpath in in_fpaths:
        cmd.append('I={}'.format(in_fpath))
    stderr = NamedTemporaryFile(suffix='picard.stderr')
    stdout = NamedTemporaryFile(suffix='picard.stdout')
    try:
        check_call(cmd, stderr=stderr, stdout=stdout)
    except CalledProcessError:
        sys.stderr.write(open(stderr.name).read())
        sys.stdout.write(open(stdout.name).read())

QUAL_TO_SUBSTRACT = 30


def downgrade_read_edges(in_fpath, out_fpath, size,
                         qual_to_substract=QUAL_TO_SUBSTRACT):
    in_sam = AlignmentFile(in_fpath)
    out_sam = AlignmentFile(out_fpath, 'wb', template=in_sam)
    for aligned_read in in_sam:
        if (aligned_read.has_tag(LEFT_DOWNGRADED_TAG) or
                aligned_read.has_tag(RIGTH_DOWNGRADED_TAG)):
            raise RuntimeError('Edge qualities already downgraded\n')

        _downgrade_edge_qualities(aligned_read, size,
                                  qual_to_substract=qual_to_substract)
        out_sam.write(aligned_read)


def _downgrade_edge_qualities(aligned_read, size, qual_to_substract):
    left_limit = aligned_read.qstart + size
    right_limit = aligned_read.qend - size
    quals = list(aligned_read.query_qualities)

    if left_limit >= right_limit:
        right_limit = left_limit + 1
    
    left_quals = quals[:left_limit]
    right_quals = quals[right_limit:]

    def minus(qual):
        qual -= qual_to_substract
        if qual < 0:
            qual = 0
        return qual

    downgraded_left_quals = list(map(minus, left_quals))
    downgraded_right_quals = list(map(minus, right_quals))

    new_quals = downgraded_left_quals
    new_quals.extend(quals[left_limit:right_limit])
    new_quals.extend(downgraded_right_quals)
    try: 
        assert len(quals) == len(new_quals)
    except AssertionError:
        print left_limit, right_limit
        print left_quals, downgraded_left_quals
        print right_quals, downgraded_right_quals
        raise
    aligned_read.query_qualities = array('B', new_quals)

    def to_sanger_qual(quals):
        return ''.join(chr(33 + qual) for qual in quals)

    aligned_read.set_tag(LEFT_DOWNGRADED_TAG, to_sanger_qual(left_quals),
                         value_type='Z')
    aligned_read.set_tag(RIGTH_DOWNGRADED_TAG, to_sanger_qual(right_quals),
                         value_type='Z')


def _restore_qual_from_tag(aligned_read):
    def to_phred_qual(quals):
        return [ord(qual) - 33 for qual in quals]
    left_quals, rigth_quals = [], []
    if aligned_read.has_tag(LEFT_DOWNGRADED_TAG):
        left_quals = aligned_read.get_tag(LEFT_DOWNGRADED_TAG)
        left_quals = to_phred_qual(left_quals)
    if aligned_read.has_tag(RIGTH_DOWNGRADED_TAG):
        rigth_quals = aligned_read.get_tag(RIGTH_DOWNGRADED_TAG)
        rigth_quals = to_phred_qual(rigth_quals)

    if left_quals or rigth_quals:
        recover_qual = left_quals
        left_limit = len(left_quals)
        rigth_limit = -len(rigth_quals) if len(rigth_quals) else None
        recover_qual += aligned_read.query_qualities[left_limit:rigth_limit]
        recover_qual += rigth_quals
        aligned_read.query_qualities = array('B', recover_qual)


def mark_duplicates(in_fpath, out_fpath=None, tmp_dir=None, metric_fpath=None,
                    stderr_fhand=None):
    if out_fpath is None:
        out_fpath = in_fpath

    if out_fpath == in_fpath:
        mark_dup_fhand = NamedTemporaryFile(suffix='.mark_dup.bam',
                                            delete=False, dir=tmp_dir)
        temp_out_fpath = mark_dup_fhand.name
    else:
        temp_out_fpath = out_fpath

    failed = _mark_duplicates(in_fpath, temp_out_fpath, metric_fpath,
                              stderr_fhand=stderr_fhand)

    if failed:
        msg = 'Mark duplicate process failed, for {}'
        raise RuntimeError(msg.format(in_fpath))

    if temp_out_fpath != out_fpath:
        shutil.move(temp_out_fpath, out_fpath)


def _mark_duplicates(in_fpath, out_fpath, metric_fpath, stderr_fhand):
    if metric_fpath is None:
        metric_fpath = '/dev/null'

    cmd = ['picard-tools', 'MarkDuplicates', 'VALIDATION_STRINGENCY=LENIENT',
           'M={}'.format(metric_fpath), 'INPUT={}'.format(in_fpath),
           'OUTPUT={}'.format(out_fpath)]
    failed = False
    try:
        check_call(cmd, stderr=stderr_fhand, stdout=stderr_fhand)
    except CalledProcessError:
        failed = True
    return failed
