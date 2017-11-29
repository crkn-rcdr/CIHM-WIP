use 5.006;
use strict;
use warnings FATAL => 'all';
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME             => 'CIHM::WIP',
    AUTHOR           => q{Russell McOrmond <russell.mcormond@canadiana.ca>},
    VERSION_FROM     => 'lib/CIHM/WIP.pm',
    ABSTRACT_FROM    => 'lib/CIHM/WIP.pm',
    LICENSE          => 'Artistic_2_0',
    PL_FILES         => {},
    "EXE_FILES"      => [
	"bin/wip", "bin/wip-imageconv", "bin/wip-metsproc", "bin/wip-move", "bin/wip-tdrexport", "bin/wip-unitize", "bin/mallet", "bin/tdringest"
    ],
    MIN_PERL_VERSION => 5.006,
    CONFIGURE_REQUIRES => {
        'ExtUtils::MakeMaker' => 0,
    },
    BUILD_REQUIRES => {
        'Test::More' => 0,
    },
    PREREQ_PM => {
        #'ABC'              => 1.6,
        #'Foo::Bar::Module' => 5.0401,
    },
    dist  => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean => { FILES => 'CIHM-WIP-*' },
);
