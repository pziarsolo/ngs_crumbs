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

'''
In this module all the imports of the optional modules are handled
'''

from crumbs.exceptions import OptionalRequirementError

MSG = 'A python package to run this executable is required,'
MSG += ' but it is not installed: '
BIO = 'biopython'
BIO_BGZF = 'biopython with Bgzf support'


def create_fake_class(msg):
    class FakePythonRequiredClass(object):
        def __init__(self, *args, **kwargs):
            raise OptionalRequirementError(msg)
    return FakePythonRequiredClass


def create_fake_funct(msg):
    def FakeRequiredfunct(*args, **kwargs):
        raise OptionalRequirementError(msg)
    return FakeRequiredfunct

try:
    from Bio.SeqRecord import SeqRecord
    from Bio.Seq import Seq
    from Bio.SeqFeature import SeqFeature, FeatureLocation
except ImportError:
    SeqRecord = create_fake_class(MSG + BIO)
    Seq = create_fake_class(MSG + BIO)
    SeqFeature = create_fake_class(MSG + BIO)
    FeatureLocation = create_fake_class(MSG + BIO)
try:
    from Bio.Restriction.Restriction import (CommOnly, RestrictionBatch,
                                             Analysis)
except ImportError:
    CommOnly = create_fake_class(MSG + BIO)
    RestrictionBatch = create_fake_class(MSG + BIO)
    Analysis = create_fake_class(MSG + BIO)
try:
    from Bio.SeqIO.SffIO import SffIterator
except ImportError:
    SffIterator = create_fake_class(MSG + BIO)

try:
    from Bio.bgzf import BgzfWriter
except ImportError:
    BgzfWriter = create_fake_class(MSG + BIO_BGZF)


try:
    from Bio.SeqIO.QualityIO import FastqGeneralIterator
except ImportError:
    FastqGeneralIterator = create_fake_class(MSG + BIO)

try:
    from Bio.SeqIO.FastaIO import FastaIterator
except ImportError:
    FastaIterator = create_fake_class(MSG + BIO)

try:
    from Bio.SeqIO.QualityIO import QualPhredIterator
    from Bio.SeqIO.QualityIO import PairedFastaQualIterator
    from Bio.SeqIO.QualityIO import FastqPhredIterator
    from Bio.SeqIO.QualityIO import FastqSolexaIterator
    from Bio.SeqIO.QualityIO import FastqIlluminaIterator
    from Bio.SeqIO import parse as parse_into_seqrecs
    from Bio.SeqIO import write as write_seqrecs
except ImportError:
    QualPhredIterator = create_fake_class(MSG + BIO)
    PairedFastaQualIterator = create_fake_class(MSG + BIO)
    FastqPhredIterator = create_fake_class(MSG + BIO)
    FastqSolexaIterator = create_fake_class(MSG + BIO)
    FastqIlluminaIterator = create_fake_class(MSG + BIO)
    parse_into_seqrecs = create_fake_funct(MSG + BIO)
    write_seqrecs = create_fake_funct(MSG + BIO)

try:
    from Bio.Blast import NCBIXML, NCBIWWW
except ImportError:
    NCBIXML = create_fake_class(MSG + BIO)
    NCBIWWW = create_fake_class(MSG + BIO)

try:
    from Bio._py3k import _bytes_to_string, _as_bytes
except ImportError:
    _bytes_to_string = create_fake_funct(MSG + BIO)
    _as_bytes = create_fake_funct(MSG + BIO)

try:
    from Bio.SeqIO._index import SeqFileRandomAccess
    from Bio.SeqIO._index import _FormatToRandomAccess
    from Bio.SeqIO import index as seq_index
except ImportError:
    SeqFileRandomAccess = create_fake_class(MSG + BIO)
    _FormatToRandomAccess = create_fake_class(MSG + BIO)
    seq_index = create_fake_class(MSG + BIO)

try:
    from Bio.Alphabet import Alphabet, AlphabetEncoder
except ImportError:
    Alphabet = create_fake_class(MSG + BIO)
    AlphabetEncoder = create_fake_class(MSG + BIO)
# toolz
try:
    from toolz.itertoolz import merge_sorted, first
except ImportError:
    merge_sorted = create_fake_funct(MSG + 'toolz')
    first = create_fake_funct(MSG + 'toolz')
# pysam
try:
    from pysam import Samfile
    from pysam import view, index, faidx, calmd
except ImportError:
    Samfile = create_fake_funct(MSG + 'pysam')
    view = create_fake_funct(MSG + 'pysam')
    index = create_fake_funct(MSG + 'pysam')
    faidx = create_fake_funct(MSG + 'pysam')
    calmd = create_fake_funct(MSG + 'pysam')

try:
    from pysam import AlignmentFile
except ImportError:
    AlignmentFile = create_fake_funct(MSG + 'pysam version >0.8')

# configobj
try:
    from configobj import ConfigObj
except ImportError:
    ConfigObj = create_fake_funct(MSG + 'configobj')

# pyvcf
try:
    from vcf.parser import _Filter, _Info
    from vcf import Reader, Writer
    from vcf.model import make_calldata_tuple, _Call, _Record
except ImportError:
    _Filter = create_fake_class(MSG + 'pyvcf')
    _Info = create_fake_class(MSG + 'pyvcf')
    Reader = create_fake_class(MSG + 'pyvcf')
    Writer = create_fake_class(MSG + 'pyvcf')
    make_calldata_tuple = create_fake_funct(MSG + 'pyvcf')
    _Call = create_fake_class(MSG + 'pyvcf')
    _Record = create_fake_class(MSG + 'pyvcf')

# numpy
try:
    from numpy import linspace, histogram, zeros, median, sum
    from numpy import absolute, exp, array, percentile
except ImportError:
    linspace = create_fake_funct(MSG + 'numpy')
    histogram = create_fake_funct(MSG + 'numpy')
    zeros = create_fake_funct(MSG + 'numpy')
    median = create_fake_funct(MSG + 'numpy')
    sum = create_fake_funct(MSG + 'numpy')
    absolute = create_fake_funct(MSG + 'numpy')
    exp = create_fake_funct(MSG + 'numpy')
    array = create_fake_funct(MSG + 'numpy')
    percentile = create_fake_funct(MSG + 'numpy')


# matplotlib
try:
    from matplotlib.mlab import griddata
    from matplotlib import cm
    from matplotlib.colorbar import make_axes, Colorbar
    from matplotlib.ticker import ScalarFormatter
    from matplotlib.colors import LogNorm, Normalize
except ImportError:
    griddata = create_fake_funct(MSG + 'matplotlib')
    cm = create_fake_funct(MSG + 'matplotlib')
    make_axes = create_fake_funct(MSG + 'matplotlib')
    Colorbar = create_fake_class(MSG + 'matplotlib')
    ScalarFormatter = create_fake_class(MSG + 'matplotlib')
    LogNorm = create_fake_class(MSG + 'matplotlib')
    Normalize = create_fake_class(MSG + 'matplotlib')

try:
    from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
    from matplotlib.figure import Figure
except ImportError as error:
    Figure = create_fake_class(MSG + 'matplotlib')
    FigureCanvas = create_fake_class(MSG + 'matplotlib')

try:
    from mpl_toolkits.axes_grid1 import make_axes_locatable
except ImportError as error:
    make_axes_locatable = create_fake_funct(MSG + 'matplotlib')

# scipy
try:
    from scipy.optimize import curve_fit
    from scipy.stats.distributions import t
    from scipy.stats import fisher_exact as scipy_fisher
except ImportError:
    curve_fit = create_fake_funct(MSG + 'scipy')
    t = create_fake_funct(MSG + 'scipy')
    scipy_fisher = create_fake_funct(MSG + 'scipy')

# rpy2
try:
    from rpy2.robjects import IntVector, r
except ImportError:
    r = create_fake_funct(MSG + 'rpy')
    IntVector = create_fake_funct(MSG + 'rpy')
