package CIHM::WIP::REST::wipmeta;

use strict;
use Carp;

use Moo;
with 'Role::REST::Client';
use Types::Standard qw(HashRef Str Int Enum HasMethods);

=head1 NAME

CIHM::WIP::REST::wipmeta - Subclass of Role::REST::Client used to
interact with "wipmeta" CouchDB databases

=head1 SYNOPSIS

    my $t_repo = CIHM::WIP::REST::wipmeta->new($args);
      where $args is a hash of arguments.  In addition to arguments
      processed by Role::REST::Client we have the following 

      $args->{conf} is as defined in CIHM::TDR::TDRConfig

=cut

sub BUILD {
    my $self = shift;
    my $args = shift;

    $self->{conf}     = $args->{conf};
    $self->{database} = $args->{database};
    $self->set_persistent_header( 'Accept' => 'application/json' );
}

# Simple accessors for now -- Do I want to Moo?
sub database {
    my $self = shift;
    return $self->{database};
}

=head1 METHODS

=head2 update_basic

    sub update_basic ( string UID, hash updatedoc )

    updatedoc - a hash that is passed to the _update function of the
        design document to update data for the given UID.
        Meaning of fields in updatedoc is defined by that function.

  returns null, or a string representing the return from the _update
  design document.  Return values include "update", "no update", "no create".


=cut

sub update_basic {
    my ( $self, $uid, $updatedoc ) = @_;
    my ( $res, $code, $data );

    $self->type("application/json");
    $res =
      $self->post( "/" . $self->database . "/_design/tdr/_update/basic/" . $uid,
        $updatedoc, { deserializer => 'application/json' } );

    if ( $res->code != 201 && $res->code != 200 ) {
        warn "_update/basic/$uid POST return code: " . $res->code . "\n";
    }
    if ( $res->data ) {
        return $res->data->{return};
    }
}

sub update_filesystem {
    my ( $self, $uid, $updatedoc ) = @_;
    my ( $res, $code, $data );

    # This encoding makes $updatedoc variables available as form data
    $self->type("application/x-www-form-urlencoded");
    $res = $self->post(
        "/" . $self->database . "/_design/tdr/_update/filesystem/" . $uid,
        $updatedoc, { deserializer => 'application/json' } );

    if ( $res->code != 201 && $res->code != 200 ) {
        warn "_update/filesystem/$uid POST return code: " . $res->code . "\n";
    }
    if ( $res->data ) {
        return $res->data;
    }
}

=head2 put_attachment

    sub pub_attachment ( string UID, hash args )

    args - a hash that can have a variety of keys
           type => MIME type of attachment (default text/json)
           filename => filename of attachment
           content => content of attachment
           updatedoc => a hash that will be passed to the _update function

  returns null (failure to get previous document revision), 
  or an integer representing the HTTP return value of the put (201 is success).

=cut

sub put_attachment {
    my ( $self, $uid, $args ) = @_;
    my ( $res, $revision, $updatedoc );

    if ( exists $args->{updatedoc} ) {
        $updatedoc = $args->{updatedoc};
    }
    else {
        $updatedoc = {};
    }

    if ( !exists $args->{type} ) {

        # Set JSON as the default attachment mime type
        $args->{type} = "text/json";
    }
    my $filename = $args->{filename};
    if ( !$uid || !$filename || !( $args->{content} ) ) {
        croak "Missing UID, filename, or content for put_attachment\n";
    }

    $res = $self->head( "/" . $self->database . "/$uid",
        {}, { deserializer => 'application/json' } );
    if ( $res->code == 200 ) {
        $revision = $res->response->header("etag");
        $revision =~ s/^\"|\"$//g;
    }
    elsif ( $res->code == 404 ) {
    }
    else {
        warn "put_attachment($uid) HEAD return code: " . $res->code . "\n";
        return;
    }
    $self->clear_headers;
    $self->set_header( 'If-Match' => $revision ) if $revision;
    $self->type( $args->{type} );
    $res = $self->put( "/" . $self->database . "/$uid/$filename",
        $args->{content}, { deserializer => 'application/json' } );
    if ( $res->code != 201 ) {
        warn "put_attachment($uid) PUT return code: " . $res->code . "\n";
    }
    else {
        $self->update_basic( $uid, $updatedoc );
    }
    return $res->code;
}

sub get_configdocs {
    my ($self) = @_;
    my ( $res, $code, $data );

    $res = $self->get(
        "/"
          . $self->database
          . "/_design/tdr/_view/configs?reduce=false&include_docs=true",
        {},
        { deserializer => 'application/json' }
    );

    if ( $res->code != 200 ) {
        warn "_view/configs GET return code: " . $res->code . "\n";
    }
    if ( $res->data && $res->data->{rows} ) {
        my $configdocs = {};
        foreach my $config ( @{ $res->data->{rows} } ) {
            $configdocs->{ $config->{'id'} } = $config->{'doc'};
        }
        return $configdocs;
    }
}

sub get_filesystem {
    my ( $self, $docs ) = @_;
    my ( $res, $code, $data );

    $res = $self->get(
        "/"
          . $self->database
          . "/_design/tdr/_view/filesystem?reduce=false&include_docs="
          . ( $docs ? "true" : "false" ),
        {},
        { deserializer => 'application/json' }
    );

    if ( $res->code != 200 ) {
        warn "_view/configs GET return code: " . $res->code . "\n";
    }
    if ( $res->data && $res->data->{rows} ) {
        return $res->data->{rows};
    }
}

sub get_aip {
    my ( $self, $uid ) = @_;

    $self->type("application/json");
    my $res = $self->get( "/" . $self->{database} . "/$uid",
        {}, { deserializer => 'application/json' } );
    if ( $res->code == 200 ) {
        return $res->data;
    }
    elsif ( $res->code == 404 ) {
        return;
    }
    else {
        warn "get_aip return code: " . $res->code . "\n";
        return;
    }
}

sub get_attachment {
    my ( $self, $uid, $attachment ) = @_;

    $self->type("application/json");
    my $res = $self->get( "/" . $self->{database} . "/$uid/$attachment",
        {}, { deserializer => 'application/json' } );
    if ( $res->code == 200 ) {
        return $res->response->content;
    }
    else {
        warn "get_aip return code: " . $res->code . "\n";
        return;
    }
}

1;
