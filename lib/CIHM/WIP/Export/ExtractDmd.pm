package CIHM::WIP::Export::ExtractDmd;

use Data::Dumper;
use XML::LibXML;

sub extract {
    my ($source,$dest) = @_;

    my $type = "physical"; #  We only ended up creating one type...

    my $metadata = XML::LibXML->load_xml(location => $source);
    my $xpc = XML::LibXML::XPathContext->new;


    my @nodes = $xpc->findnodes("descendant::mets:structMap[\@TYPE=\"$type\"]",$metadata);
    if (scalar(@nodes) != 1) {
	die "Found ".scalar(@nodes)." structMap(TYPE=$type)\n";
    }
    my @divs=$xpc->findnodes('descendant::mets:div',$nodes[0]);
    if (! scalar(@divs)) {
	die "Couldn't find structMap divs\n";
    };
    my $dmdid=$divs[0]->getAttribute('DMDID');
    if (!$dmdid) {
	die "Couldn't find DMDID in first structMap div\n";
    };
    my @dmdsec=$xpc->findnodes("descendant::mets:dmdSec[\@ID=\"$dmdid\"]",$metadata);
    if (scalar(@dmdsec) != 1) {
	die "Found ".scalar(@dmdsec)." dmdSec for ID=$dmdid\n";
    }
    my @md=$dmdsec[0]->nonBlankChildNodes();
    if (scalar(@md) != 1) {
	die "Found ".scalar(@md)." children for dmdSec ID=$dmdid\n";
    }
    my $mdtype=$md[0]->getAttribute('MDTYPE');
    if ($mdtype eq 'OTHER') {
	$mdtype=$md[0]->getAttribute('OTHERMDTYPE');
    }
    # unWrap the data  -- always in form of  <mets:mdWrap><mets:xmlData>
    my $dmd=(($md[0]->nonBlankChildNodes())[0]->nonBlankChildNodes())[0];

    my $outfile=$dest."-".$mdtype.".xml";

    open(my $fh, '>:encoding(UTF-8)', $outfile) or 
	die "Could not open destination '$outfile' $!";

    print $fh $dmd->toString();
    close $fh;
}

1;
