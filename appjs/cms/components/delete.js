/*
 * Copyright © LOGILAB S.A. (Paris, FRANCE) 2016-2019
 * Contact http://www.logilab.fr -- mailto:contact@logilab.fr
 *
 * This software is governed by the CeCILL-C license under French law and
 * abiding by the rules of distribution of free software. You can use,
 * modify and/ or redistribute the software under the terms of the CeCILL-C
 * license as circulated by CEA, CNRS and INRIA at the following URL
 * "http://www.cecill.info".
 *
 * As a counterpart to the access to the source code and rights to copy,
 * modify and redistribute granted by the license, users are provided only
 * with a limited warranty and the software's author, the holder of the
 * economic rights, and the successive licensors have only limited liability.
 *
 * In this respect, the user's attention is drawn to the risks associated
 * with loading, using, modifying and/or developing or reproducing the
 * software by the user in light of its specific status of free software,
 * that may mean that it is complicated to manipulate, and that also
 * therefore means that it is reserved for developers and experienced
 * professionals having in-depth computer knowledge. Users are therefore
 * encouraged to load and test the software's suitability as regards their
 * requirements in conditions enabling the security of their systemsand/or
 * data to be ensured and, more generally, to use and operate it in the
 * same conditions as regards security.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL-C license and that you accept its terms.
 */
const React = require('react')

const {Component} = require('react'),
    PropTypes = require('prop-types')

const {
        default: {getEntity, deleteEntity},
    } = require('../api'),
    {spinner} = require('../components/fa')
const {parse} = require('query-string')

function renderValidationError(error) {
    let attrname
    if (error.hasOwnProperty('source')) {
        attrname = error.source.pointer
    }
    return [attrname, error.details || error.title].join(': ')
}

const location_reload = document.location.reload.bind(document.location)

class DeleteForm extends Component {
    constructor(props) {
        super(props)
        this.state = {errors: null, loading: true, deleting: false}
        this.entity = props.entity.toJS()
        this.deleteEntity = this.deleteEntity.bind(this)
    }

    UNSAFE_componentWillReceiveProps(nextProps) {
        const nextEntity = nextProps.entity.toJS(),
            query = parse(nextProps.location.search),
            cw_etype = query.cwetype || nextEntity.cw_etype,
            eid = query.eid || nextEntity.eid
        if (this.entity.eid === eid) {
            return
        }
        this.entity = nextEntity
        this.fetchData(eid, cw_etype)
    }

    fetchData(eid, cw_etype) {
        getEntity(cw_etype, eid).then(entity =>
            this.setState({entityToDelete: entity, loading: false}),
        )
    }

    componentDidMount() {
        const query = parse(this.props.location.search)
        const cw_etype = query.cw_etype || this.entity.cw_etype,
            eid = query.eid || this.entity.eid
        this.fetchData(eid, cw_etype)
    }

    deleteEntity() {
        const {cw_etype, eid} = this.state.entityToDelete
        this.setState({deleteting: true})
        deleteEntity(cw_etype, eid).then(res => {
            if (res.errors && res.errors.length) {
                this.setState({errors: res.errors})
                return
            } else {
                document.location.reload()
            }
        })
    }

    render() {
        const {loading, entityToDelete, deleteting} = this.state
        if (loading) {
            return <spinner />
        }
        if (this.state.errors !== null) {
            const errors = this.state.errors.map(error => (
                <div key={error} className="alert alert-danger">
                    {renderValidationError(error)}
                </div>
            ))
            return (
                <div>
                    <h2>Suppression impossible</h2>
                    {errors}
                </div>
            )
        } else {
            const title = entityToDelete.dc_title || entityToDelete.cw_etype,
                href =
                    window.BASE_URL +
                    entityToDelete.cw_etype +
                    '/' +
                    entityToDelete.eid
            return (
                <div>
                    <h2>Suppression du document "{title}" </h2>
                    <div className="alert alert-info">
                        Voulez-vous vraiment supprimer l'entité{' '}
                        {entityToDelete.cw_etype} "
                        <a
                            href={href}
                            rel="noopener noreferrer"
                            target="_blank"
                        >
                            "{title}"
                        </a>{' '}
                        ?
                    </div>
                    <button
                        type="button"
                        className="btn btn-default"
                        onClick={location_reload}
                    >
                        annuler
                    </button>

                    <button
                        type="submit"
                        className="btn btn-primary"
                        onClick={this.deleteEntity}
                    >
                        {deleteting ? <spinner /> : null}
                        supprimer
                    </button>
                </div>
            )
        }
    }
}
DeleteForm.propTypes = {
    entity: PropTypes.object,
    location: PropTypes.shape({
        search: PropTypes.object,
        query: PropTypes.shape({
            eid: PropTypes.string,
            cwetype: PropTypes.string,
        }),
    }),
}

module.exports = DeleteForm
